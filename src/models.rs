use std::str::FromStr;
use std::{future::Future, pin::Pin};

use anyhow::{Result, anyhow};
use chrono::{DateTime, Duration, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use stripe::{
    CancelSubscription, CheckoutSession as StripeCheckoutSession, CheckoutSessionId,
    CheckoutSessionStatus, CreateCheckoutSession, CreateCheckoutSessionAutomaticTax, CustomerId,
    Event, EventObject, EventType, Expandable, SubscriptionId, SubscriptionSchedule,
};
use uuid::Uuid;

use crate::tables::{
    BillingLink, CheckoutSession, PricingPlan, SubscriptionState, SubscriptionStateUpdate,
    SubscriptionType,
};

pub type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;

#[derive(Deserialize)]
pub struct FinalizeCheckout {
    pub session_id: String,
}

#[derive(Deserialize, Debug, PartialEq, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionPeriod {
    OneTime,
    Monthly,
    Yearly,
}

impl FromStr for SubscriptionPeriod {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "one_time" => Ok(SubscriptionPeriod::OneTime),
            "monthly" => Ok(SubscriptionPeriod::Monthly),
            "yearly" => Ok(SubscriptionPeriod::Yearly),
            _ => Err(format!("Invalid subscription type: {}", s)),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiProduct {
    key: String,
    name: String,
    description: String,
    unit_amount: u32,
    currency: stripe::Currency,
    subscription: SubscriptionPeriod,
    #[serde(skip_serializing_if = "Option::is_none")]
    credits: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    trial_period_days: Option<u32>,
}

fn stripe_client_from_env() -> Result<stripe::Client> {
    let stripe_key =
        std::env::var("STRIPE_API_KEY").map_err(|_| anyhow!("Missing STRIPE_API_KEY"))?;
    Ok(stripe::Client::new(stripe_key))
}

fn parse_u64_opt(s: Option<&str>) -> Option<u64> {
    s.and_then(|v| v.parse::<u64>().ok())
}

/// Fetch a single product for a plan key.
/// `fetch_plan_ref` should return the Stripe `price_id` you want to sell for that plan key
/// (usually from a tiny DB table or config).
pub async fn get_product<F>(plan_key: &str, fetch_plan: &F) -> Result<ApiProduct>
where
    F: for<'a> Fn(&'a str) -> BoxFut<Result<PricingPlan>> + Send + Sync,
{
    let plan = fetch_plan(plan_key).await?;
    let client = stripe_client_from_env()?;

    let price_id = stripe::PriceId::from_str(&plan.price_id)
        .map_err(|_| anyhow!("Invalid Stripe price id: {}", plan.price_id))?;

    // Expand product so we can read name/description/metadata in one call.
    let expand: &[&str] = &["product"];
    let price = stripe::Price::retrieve(&client, &price_id, expand)
        .await
        .map_err(|e| anyhow!("Stripe price retrieve failed: {e}"))?;

    let currency = price
        .currency
        .ok_or_else(|| anyhow!("Stripe price missing currency"))?;

    // For “flat” prices this is Some. For usage/tiered, it can be None.
    let unit_amount = price
        .unit_amount
        .or(price
            .unit_amount_decimal
            .as_ref()
            .and_then(|s| s.parse::<i64>().ok()))
        .ok_or_else(|| anyhow!("Stripe price missing unit_amount (usage/tiered price?)"))?;

    let subscription = match price.recurring.as_ref().map(|r| r.interval) {
        Some(stripe::RecurringInterval::Year) => SubscriptionPeriod::Yearly,
        Some(_) => SubscriptionPeriod::Monthly, // month/week/day/etc -> bucket as “recurring”
        None => SubscriptionPeriod::OneTime,
    };

    // Product details (expanded) or fallback to a second call if it wasn't expanded for some reason.
    let product = match price.product.clone() {
        Some(stripe::Expandable::Object(p)) => Some(*p),
        Some(stripe::Expandable::Id(pid)) => Some(
            stripe::Product::retrieve(&client, &pid, &[])
                .await
                .map_err(|e| anyhow!("Stripe product retrieve failed: {e}"))?,
        ),
        None => None,
    };

    let name: String = product
        .as_ref()
        .map(|p| p.name.clone().or_else(|| price.nickname.clone()))
        .flatten()
        .unwrap_or_else(|| plan.key.clone());

    let description = product.as_ref().and_then(|p| p.description.clone());

    // Credits: recommend storing on Product metadata (or Price metadata) as "credits".
    // If you want plan-specific credits per price, prefer price.metadata.
    let credits = {
        let metadata = price.metadata.as_ref();
        let credits = metadata.map(|m| m.get("credits").map(|s| s.as_str()));
        let from_price = parse_u64_opt(credits.flatten());

        let metadata = product.as_ref().and_then(|p| p.metadata.as_ref());
        let credits = metadata.map(|m| m.get("credits").map(|s| s.as_str()));
        let from_product = parse_u64_opt(credits.flatten());
        from_price.or(from_product)
    };

    Ok(ApiProduct {
        key: plan.key,
        name,
        description: description.unwrap_or_default(),
        unit_amount: unit_amount as u32,
        currency,
        subscription,
        credits: credits.map(|c| c as i32),
        trial_period_days: None,
    })
}

pub async fn get_products<F, Fut, G>(fetch_all: F, fetch_plan: &G) -> Result<Vec<ApiProduct>>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<Vec<PricingPlan>>> + Send,
    G: for<'a> Fn(&'a str) -> BoxFut<Result<PricingPlan>> + Send + Sync,
{
    let plans = fetch_all().await?;

    let mut products: Vec<ApiProduct> = Vec::with_capacity(plans.len());
    for plan in plans {
        let product = get_product(&plan.key, fetch_plan).await?;
        products.push(product);
    }

    Ok(products)
}

pub async fn checkout_status<F, Fut, G, GFut, H, HFut, I, IFut, J, JFut>(
    params: FinalizeCheckout,
    get_checkout_session: F,
    upsert_billing_link: G,
    update_subscription_from_checkout: H,
    add_credits_to_plan: I,
    delete_checkout_session: J,
) -> Result<Value>
where
    F: FnOnce(Uuid) -> Fut,
    Fut: Future<Output = Result<CheckoutSession>>,
    G: FnOnce(BillingLink) -> GFut,
    GFut: Future<Output = Result<()>>,
    H: FnOnce(Uuid, stripe::Subscription) -> HFut,
    HFut: Future<Output = Result<()>>,
    I: Fn(Uuid, &str, Option<u64>) -> IFut,
    IFut: Future<Output = Result<()>>,
    J: FnOnce(Uuid) -> JFut,
    JFut: Future<Output = Result<()>>,
{
    tracing::info!("Checkout status: {:?}", params.session_id);
    let stripe_key = match std::env::var("STRIPE_API_KEY") {
        Ok(key) => key,
        Err(_) => return Err(anyhow!("Missing STRIPE_API_KEY")),
    };
    let client = stripe::Client::new(stripe_key);
    let session_id = CheckoutSessionId::from_str(&params.session_id).map_err(|err| {
        tracing::error!("Error parsing session ID: {}", err);
        anyhow!("Invalid Session ID")
    })?;
    let session = StripeCheckoutSession::retrieve(&client, &session_id, &[])
        .await
        .map_err(|err| {
            tracing::error!("Error retrieving checkout session: {}", err);
            anyhow!("Checkout session error")
        })?;
    let client_reference_id = session.client_reference_id.unwrap_or_default();
    let checkout_session_id = Uuid::parse_str(&client_reference_id).map_err(|err| {
        tracing::error!("Error parsing checkout session ID: {}", err);
        anyhow!("Invalid checkout ID")
    })?;
    let checkout = get_checkout_session(checkout_session_id).await?;

    let status = session.status;
    let customer_email = session.customer_email.clone();
    if status == Some(CheckoutSessionStatus::Complete) {
        let customer_id = session.customer.unwrap_or_default().id();
        let internal_id = checkout.internal_id;
        let billing_link = BillingLink::new(&customer_id, internal_id);
        upsert_billing_link(billing_link).await.ok();

        let StripeCheckoutSession {
            subscription,
            line_items,
            ..
        } = session;
        match subscription {
            // We don't construct carts with mixed subscriptions and one-time purchases
            Some(sub_exp) => {
                let sub = match sub_exp {
                    Expandable::Id(id) => stripe::Subscription::retrieve(&client, &id, &[])
                        .await
                        .map_err(|err| {
                        tracing::error!("Error retrieving subscription: {}", err);
                        anyhow!("Subscription error")
                    })?,
                    Expandable::Object(sub) => *sub,
                };
                tracing::info!("Org({}) subscribed: {:?}", internal_id, sub.id.as_str());
                update_subscription_from_checkout(checkout.internal_id, sub).await?;
            }
            // If no subscription, add extra credits for the one-time purchase
            None => {
                for item in line_items.unwrap_or_default().data {
                    let product_id = item
                        .price
                        .as_ref()
                        .and_then(|price| price.product.as_ref())
                        .and_then(|product| match product {
                            Expandable::Id(id) => Some(id),
                            Expandable::Object(product) => Some(&product.id),
                        });
                    if let Some(product_id) = product_id {
                        add_credits_to_plan(checkout.internal_id, &product_id, item.quantity)
                            .await?;
                    }
                }
            }
        };
    } else {
        return Err(anyhow!("Checkout session not completed: {:?}", status));
    }
    delete_checkout_session(checkout_session_id).await.ok();

    Ok(json!({
        "status": status.unwrap_or(CheckoutSessionStatus::Expired),
        "customerEmail": customer_email.unwrap_or("".to_string()),
    }))
}

pub async fn create_checkout_cart<F, Fut, G, GFut>(
    internal_id: Uuid,
    base_url: &str,
    stripe_return_uri: &str,
    plan: &PricingPlan,
    quantity: u64,
    get_customer_id: F,
    create_checkout_session: G,
) -> Result<String>
where
    F: FnOnce(Uuid) -> Fut,
    Fut: Future<Output = Option<String>>,
    G: FnOnce(CheckoutSession) -> GFut,
    GFut: Future<Output = ()>,
{
    let client = stripe_client_from_env()?;
    let customer_id = get_customer_id(internal_id).await;

    let price_id = stripe::PriceId::from_str(&plan.price_id)
        .map_err(|_| anyhow!("Invalid Stripe price id: {}", plan.price_id))?;
    let expand: &[&str] = &["product"];
    let price = stripe::Price::retrieve(&client, &price_id, expand)
        .await
        .map_err(|e| anyhow!("Stripe price retrieve failed: {e}"))?;

    let mut params = CreateCheckoutSession::new();
    let auto_tax = CreateCheckoutSessionAutomaticTax {
        enabled: true,
        liability: None,
    };

    let client_reference_id = Uuid::new_v4();
    let session = CheckoutSession::new(client_reference_id, internal_id);
    create_checkout_session(session).await;

    let client_reference_id = client_reference_id.to_string();
    params.client_reference_id = Some(&client_reference_id);
    params.discounts = None;
    params.customer = customer_id.map(|s| CustomerId::from_str(&s).ok()).flatten();

    params.automatic_tax = Some(auto_tax);
    params.ui_mode = Some(stripe::CheckoutSessionUiMode::Embedded);
    params.mode = Some(match price.recurring {
        Some(_) => stripe::CheckoutSessionMode::Subscription,
        None => stripe::CheckoutSessionMode::Payment,
    });
    params.line_items = Some(vec![stripe::CreateCheckoutSessionLineItems {
        price: Some(plan.price_id.clone()),
        quantity: Some(quantity),
        ..Default::default()
    }]);
    let return_url = format!(
        "{}{}?session_id={{CHECKOUT_SESSION_ID}}",
        base_url, stripe_return_uri
    );
    params.return_url = Some(&return_url);
    let stripe_session = stripe::CheckoutSession::create(&client, params)
        .await
        .map_err(|err| {
            tracing::error!("Error creating checkout session: {:?}", err);
            anyhow!("Error creating checkout session")
        })?;
    let secret = stripe_session.client_secret.ok_or_else(|| {
        tracing::error!("Error fetching client secret");
        anyhow!("Error fetching client secret")
    })?;
    Ok(secret)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionInfo {
    pub plan: SubscriptionType,
    pub seats: Option<i32>,
    pub is_active: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_period_end: Option<DateTime<Utc>>,
    pub auto_renew: bool,
}

pub async fn get_subscription_info<F, Fut>(
    internal_id: Uuid,
    get_subscription: F,
) -> Result<SubscriptionInfo>
where
    F: FnOnce(Uuid) -> Fut,
    Fut: Future<Output = Result<SubscriptionState>>,
{
    let plan = get_subscription(internal_id).await?;

    Ok(SubscriptionInfo {
        plan: plan.subscription_type,
        seats: plan.seats,
        is_active: plan.is_active,
        current_period_end: plan.current_period_end.map(|d| d.and_utc()),
        auto_renew: plan.is_auto_billing,
    })
}

pub async fn deactivate_subscription<F, Fut, G, GFut>(
    internal_id: Uuid,
    get_subscription: F,
    cancel_subscription: G,
) -> Result<()>
where
    F: FnOnce(Uuid) -> Fut,
    Fut: Future<Output = Result<SubscriptionState>>,
    G: FnOnce(Uuid) -> GFut,
    GFut: Future<Output = Result<()>>,
{
    let sub = get_subscription(internal_id).await?;
    let sub_id = sub
        .subscription_id
        .ok_or_else(|| anyhow!("No subscription for id {}", internal_id))?;

    let sub_id = SubscriptionId::from_str(&sub_id).map_err(|err| {
        tracing::error!("Error parsing subscription ID: {}", err);
        anyhow!("Error parsing subscription ID")
    })?;
    let client = stripe_client_from_env()?;

    stripe::Subscription::cancel(&client, &sub_id, CancelSubscription::default())
        .await
        .map_err(|err| {
            tracing::error!("Error cancelling subscription: {}", err);
            anyhow!("Error cancelling subscription")
        })?;

    cancel_subscription(internal_id).await?;
    Ok(())
}

fn now_unix_seconds() -> i64 {
    Utc::now().timestamp()
}

fn unix_seconds_to_systemtime(ts: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(ts, 0).single().unwrap()
}

fn remaining_until_unix(ts: i64) -> Duration {
    let now = now_unix_seconds();
    if ts <= now {
        Duration::seconds(0)
    } else {
        Duration::seconds((ts - now) as i64)
    }
}

pub fn seats_from_subscription_first_licensed(sub: &stripe::Subscription) -> Option<u64> {
    sub.items
        .data
        .iter()
        .find(|it| {
            it.price
                .as_ref()
                .and_then(|p| p.recurring.as_ref())
                .map(|r| r.usage_type)
                == Some(stripe::RecurringUsageType::Licensed)
        })
        .and_then(|it| it.quantity)
        .map(|q| q.max(1) as u64)
}

pub async fn subscription_updated<F, Fut, G, GFut, H>(
    schedule: SubscriptionSchedule,
    get_billing_link: F,
    upsert_subscription_state: G,
    update_subscription: H,
) -> Result<()>
where
    F: FnOnce(&str) -> Fut,
    Fut: Future<Output = Option<Uuid>>,
    G: FnOnce(Uuid, Duration) -> GFut,
    GFut: Future<Output = Result<()>>,
    H: FnOnce(Uuid, SubscriptionStateUpdate) -> GFut,
{
    // 1) Identify the customer on the schedule
    let customer_id = match schedule.customer {
        Expandable::Id(id) => id,
        Expandable::Object(cust) => cust.id.clone(),
    };

    // 2) Map to internal_id (if we don't know this customer, do nothing)
    let internal_id = match get_billing_link(&customer_id).await {
        Some(id) => id,
        None => return Ok(()),
    };

    // 3) Resolve the actual subscription object (source of truth)
    let subscription_id = match schedule.subscription {
        Some(Expandable::Id(id)) => id,
        Some(Expandable::Object(sub)) => sub.id.clone(),
        None => return Ok(()), // schedule exists but not attached to a subscription yet
    };

    let client = stripe_client_from_env()?;
    let sub = stripe::Subscription::retrieve(&client, &subscription_id, &[])
        .await
        .map_err(|e| anyhow!("Stripe subscription retrieve failed: {e}"))?;

    // 4) Compute remaining access window (or 0 if ended)
    let remaining = remaining_until_unix(sub.current_period_end);
    let seats = seats_from_subscription_first_licensed(&sub);

    // 5) Build a compact state update payload for your DB
    let update = SubscriptionStateUpdate {
        subscription_id: Some(sub.id.to_string()),
        subscription_type: SubscriptionType::Paid,
        cancel_at_period_end: sub.cancel_at_period_end,
        current_period_end: Some(unix_seconds_to_systemtime(sub.current_period_end).naive_utc()),
        is_auto_billing: !sub.cancel_at_period_end,
        seats,
    };

    // 6) Persist idempotently
    upsert_subscription_state(internal_id, remaining).await?;
    update_subscription(internal_id, update).await?;
    Ok(())
}

pub async fn handle_stripe_event<F, Fut, G, GFut, H, I, IFut, J, K, L>(
    event: Event,
    get_billing_link: F,
    set_default_credits_and_seats: G,
    inactivate_subscription: H,
    get_subscription: I,
    cancel_subscription: J,
    upsert_subscription_state: K,
    update_subscription: L,
) -> Result<()>
where
    F: FnOnce(&str) -> Fut,
    Fut: Future<Output = Option<Uuid>>,
    G: Fn(Uuid) -> GFut,
    GFut: Future<Output = Result<()>>,
    H: Fn(&str, bool) -> GFut,
    I: FnOnce(Uuid) -> IFut,
    IFut: Future<Output = Result<SubscriptionState>>,
    J: FnOnce(Uuid) -> GFut,
    K: FnOnce(Uuid, Duration) -> GFut,
    L: FnOnce(Uuid, SubscriptionStateUpdate) -> GFut,
{
    match event.type_ {
        EventType::SubscriptionScheduleAborted | EventType::SubscriptionScheduleCompleted => {
            let sub = match event.data.object {
                EventObject::SubscriptionSchedule(sub) => sub,
                _ => return Ok(()),
            };
            let sub_id = sub.id.as_str();
            inactivate_subscription(sub_id, true).await?;
            let internal_id = get_billing_link(sub.customer.id().as_str())
                .await
                .ok_or_else(|| {
                    tracing::error!("Error fetching billing link for subscription {}", sub_id);
                    anyhow!("Error fetching billing link")
                })?;
            set_default_credits_and_seats(internal_id).await?;
        }
        EventType::SubscriptionScheduleCanceled => {
            let sub = match event.data.object {
                EventObject::SubscriptionSchedule(sub) => sub,
                _ => return Ok(()),
            };
            let internal_id = get_billing_link(sub.customer.id().as_str())
                .await
                .ok_or_else(|| {
                    tracing::error!(
                        "Error fetching billing link for subscription {}",
                        sub.customer.id()
                    );
                    anyhow!("Error fetching billing link")
                })?;
            deactivate_subscription(internal_id, get_subscription, cancel_subscription).await?;
        }
        EventType::SubscriptionScheduleCreated => {
            let sub = match event.data.object {
                EventObject::SubscriptionSchedule(sub) => sub,
                _ => return Ok(()),
            };
            subscription_updated(
                sub,
                get_billing_link,
                upsert_subscription_state,
                update_subscription,
            )
            .await?;
        }
        EventType::SubscriptionScheduleUpdated => {
            let sub = match event.data.object {
                EventObject::SubscriptionSchedule(sub) => sub,
                _ => return Ok(()),
            };
            subscription_updated(
                sub,
                get_billing_link,
                upsert_subscription_state,
                update_subscription,
            )
            .await?;
        }
        _ => {} // ignore irrelevant events
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_compile_get_product() {
        fn fetch_plan(key: &str) -> BoxFut<Result<PricingPlan>> {
            let key = key.to_string();
            Box::pin(async move {
                Ok(PricingPlan {
                    key,
                    price_id: "price_123".to_string(),
                })
            })
        }

        let _ = get_product("test", &fetch_plan);
    }
}
