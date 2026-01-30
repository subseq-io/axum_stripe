use std::str::FromStr;
use std::{future::Future, pin::Pin};

use anyhow::anyhow;
use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use stripe::{
    CancelSubscription, CheckoutSession as StripeCheckoutSession, CheckoutSessionId,
    CheckoutSessionStatus, CreateCheckoutSession, CreateCheckoutSessionAutomaticTax, CustomerId,
    Event, EventObject, EventType, Expandable, SubscriptionId, SubscriptionSchedule,
};
use uuid::Uuid;

use crate::error::{LibError, Result};
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
    type Err = LibError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "one_time" => Ok(SubscriptionPeriod::OneTime),
            "monthly" => Ok(SubscriptionPeriod::Monthly),
            "yearly" => Ok(SubscriptionPeriod::Yearly),
            _ => Err(LibError::invalid(
                "Invalid subscription type",
                anyhow!("SubscriptionPeriod::from_str failed {}", s),
            )),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiProduct {
    key: String,
    name: String,
    description: String,
    currency: stripe::Currency,
    subscription: SubscriptionPeriod,
    #[serde(skip_serializing_if = "Option::is_none")]
    credits: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pricing: ApiPricing,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum ApiPricing {
    FlatRate {
        unit_amount: u32,
    },
    Package {
        unit_amount: u32,
        divide_by: u64,
        round: ApiTransformRound,
    },
    Tiered {
        #[serde(skip_serializing_if = "Option::is_none")]
        tiers_mode: Option<ApiTiersMode>,
        tiers: Vec<stripe::PriceTier>,
    },
    Usage {
        #[serde(skip_serializing_if = "Option::is_none")]
        unit_amount: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        aggregate_usage: Option<ApiAggregateUsage>,
    },
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ApiTransformRound {
    Up,
    Down,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ApiTiersMode {
    Graduated,
    Volume,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ApiAggregateUsage {
    Sum,
    LastDuringPeriod,
    LastEver,
    Max,
}

pub fn stripe_client_from_env() -> Result<stripe::Client> {
    let stripe_key = std::env::var("STRIPE_API_KEY").map_err(|_| {
        LibError::upstream(
            "Server misconfiguration",
            anyhow!("stripe_client_from_env missing STRIPE_API_KEY"),
        )
    })?;
    Ok(stripe::Client::new(stripe_key))
}

fn parse_u64_opt(s: Option<&str>) -> Option<u64> {
    s.and_then(|v| v.parse::<u64>().ok())
}

fn parse_unit_amount_u32(price: &stripe::Price) -> Option<u32> {
    let i: Option<i64> = price.unit_amount.or(price
        .unit_amount_decimal
        .as_ref()
        .and_then(|s| s.parse::<i64>().ok()));
    i.and_then(|v| u32::try_from(v).ok())
}

fn v_get<'a>(v: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut cur: &Value = v;
    for k in path {
        cur = cur.get(*k)?;
    }
    Some(cur)
}

fn v_str<'a>(v: &'a Value, path: &[&str]) -> Option<&'a str> {
    v_get(v, path).and_then(|x| x.as_str())
}

fn v_u64(v: &Value, path: &[&str]) -> Option<u64> {
    v_get(v, path).and_then(|x| x.as_u64())
}

fn classify_pricing(price: &stripe::Price) -> Result<ApiPricing> {
    let raw: Value = serde_json::to_value(price).map_err(|e| {
        LibError::upstream(
            "Failed to parse product information",
            anyhow!("classify_pricing serde_json::to_value failed: {e}"),
        )
    })?;

    // Stripe shape:
    // - recurring.usage_type: "metered" => Usage
    // - billing_scheme: "tiered" => Tiered
    // - transform_quantity exists => Package
    // - else => FlatRate
    let usage_type: Option<&str> = v_str(&raw, &["recurring", "usage_type"]);
    if usage_type == Some("metered") {
        let aggregate_usage: Option<ApiAggregateUsage> =
            match v_str(&raw, &["recurring", "aggregate_usage"]) {
                Some("sum") => Some(ApiAggregateUsage::Sum),
                Some("last_during_period") => Some(ApiAggregateUsage::LastDuringPeriod),
                Some("last_ever") => Some(ApiAggregateUsage::LastEver),
                Some("max") => Some(ApiAggregateUsage::Max),
                _ => None,
            };

        return Ok(ApiPricing::Usage {
            unit_amount: parse_unit_amount_u32(price),
            aggregate_usage,
        });
    }

    let billing_scheme: Option<&str> = v_str(&raw, &["billing_scheme"]);
    if billing_scheme == Some("tiered") {
        let tiers: Vec<stripe::PriceTier> = price.tiers.clone().unwrap_or_default();
        if tiers.is_empty() {
            return Err(LibError::upstream(
                "Failed to retrieve product information",
                anyhow!("tiered price missing tiers"),
            ));
        }

        let tiers_mode: Option<ApiTiersMode> = match v_str(&raw, &["tiers_mode"]) {
            Some("graduated") => Some(ApiTiersMode::Graduated),
            Some("volume") => Some(ApiTiersMode::Volume),
            _ => None,
        };

        return Ok(ApiPricing::Tiered { tiers_mode, tiers });
    }

    if v_get(&raw, &["transform_quantity"]).is_some() {
        let unit_amount: u32 = parse_unit_amount_u32(price).ok_or_else(|| {
            LibError::upstream(
                "Failed to retrieve product information",
                anyhow!("package price missing unit_amount"),
            )
        })?;

        let divide_by: u64 =
            v_u64(&raw, &["transform_quantity", "divide_by"]).ok_or_else(|| {
                LibError::upstream(
                    "Failed to retrieve product information",
                    anyhow!("package price missing transform_quantity.divide_by"),
                )
            })?;

        let round: ApiTransformRound = match v_str(&raw, &["transform_quantity", "round"]) {
            Some("up") => ApiTransformRound::Up,
            Some("down") => ApiTransformRound::Down,
            other => {
                return Err(LibError::upstream(
                    "Failed to retrieve product information",
                    anyhow!("package price invalid transform_quantity.round: {other:?}"),
                ));
            }
        };

        return Ok(ApiPricing::Package {
            unit_amount,
            divide_by,
            round,
        });
    }

    // Flat rate per-unit pricing
    let unit_amount: u32 = parse_unit_amount_u32(price).ok_or_else(|| {
        LibError::upstream(
            "Failed to retrieve product information",
            anyhow!("flat price missing unit_amount"),
        )
    })?;
    Ok(ApiPricing::FlatRate { unit_amount })
}

/// Fetch a single product for a plan key.
/// `fetch_plan` should return the Stripe `price_id` you want to sell for that plan key
/// (usually from a tiny DB table or config).
pub async fn get_product<F>(plan_key: &str, fetch_plan: &F) -> Result<ApiProduct>
where
    F: for<'a> Fn(&'a str) -> BoxFut<Result<PricingPlan>> + Send + Sync,
{
    let plan: PricingPlan = fetch_plan(plan_key).await?;
    let client: stripe::Client = stripe_client_from_env()?;

    let price_id: stripe::PriceId = stripe::PriceId::from_str(&plan.price_id).map_err(|_| {
        LibError::invalid(
            "Invalid pricing plan configuration",
            anyhow!("get_product invalid Stripe price id: {}", plan.price_id),
        )
    })?;

    let expand: &[&str] = &["product"];
    let price: stripe::Price = stripe::Price::retrieve(&client, &price_id, expand)
        .await
        .map_err(|e| {
            LibError::upstream(
                "Failed to retrieve product information",
                anyhow!("get_product stripe price retrieve failed: {e}"),
            )
        })?;

    let currency: stripe::Currency = price.currency.ok_or_else(|| {
        LibError::upstream(
            "Failed to retrieve product information",
            anyhow!("get_product stripe price missing currency"),
        )
    })?;

    let subscription: SubscriptionPeriod = match price.recurring.as_ref().map(|r| r.interval) {
        Some(stripe::RecurringInterval::Year) => SubscriptionPeriod::Yearly,
        Some(_) => SubscriptionPeriod::Monthly,
        None => SubscriptionPeriod::OneTime,
    };

    let product: Option<stripe::Product> = match price.product.clone() {
        Some(stripe::Expandable::Object(p)) => Some(*p),
        Some(stripe::Expandable::Id(pid)) => Some(
            stripe::Product::retrieve(&client, &pid, &[])
                .await
                .map_err(|e| {
                    LibError::upstream(
                        "Failed to retrieve product information",
                        anyhow!("get_product stripe product retrieve failed: {e}"),
                    )
                })?,
        ),
        None => None,
    };

    let name: String = product
        .as_ref()
        .map(|p| p.name.clone().or_else(|| price.nickname.clone()))
        .flatten()
        .unwrap_or_else(|| plan.key.clone());
    let description: String = product
        .as_ref()
        .and_then(|p| p.description.clone())
        .unwrap_or_default();
    let credits: Option<i32> = credits_from_price(&client, &price).await;
    let pricing: ApiPricing = classify_pricing(&price)?;

    Ok(ApiProduct {
        key: plan.key,
        name,
        description,
        currency,
        subscription,
        credits,
        pricing,
    })
}

pub async fn credits_from_price(client: &stripe::Client, price: &stripe::Price) -> Option<i32> {
    let product = match price.product.clone() {
        Some(stripe::Expandable::Object(p)) => Some(*p),
        Some(stripe::Expandable::Id(pid)) => {
            stripe::Product::retrieve(&client, &pid, &[]).await.ok()
        }
        None => None,
    };

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

    credits.map(|c| c as i32)
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
    I: Fn(Uuid, stripe::PriceId, Option<u64>) -> IFut,
    IFut: Future<Output = Result<()>>,
    J: FnOnce(Uuid) -> JFut,
    JFut: Future<Output = Result<()>>,
{
    tracing::info!("Checkout status: {:?}", params.session_id);
    let client = stripe_client_from_env()?;
    let session_id = CheckoutSessionId::from_str(&params.session_id).map_err(|err| {
        LibError::invalid(
            "Invalid Session ID",
            anyhow!("checkout_status error parsing session ID: {}", err),
        )
    })?;
    let session = StripeCheckoutSession::retrieve(&client, &session_id, &[])
        .await
        .map_err(|err| {
            tracing::error!("Error retrieving checkout session: {}", err);
            LibError::upstream(
                "Failed to retrieve checkout session",
                anyhow!("checkout_status session retrieval error: {}", err),
            )
        })?;
    let client_reference_id = session.client_reference_id.unwrap_or_default();
    let checkout_session_id = Uuid::parse_str(&client_reference_id).map_err(|err| {
        tracing::error!("Error parsing checkout session ID: {}", err);
        LibError::invalid(
            "Invalid checkout session",
            anyhow!("checkout_status invalid client reference ID: {}", err),
        )
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
                        LibError::upstream(
                            "Failed to retrieve subscription",
                            anyhow!("checkout_status subscription retrieval error: {}", err),
                        )
                    })?,
                    Expandable::Object(sub) => *sub,
                };
                tracing::info!("Org({}) subscribed: {:?}", internal_id, sub.id.as_str());
                update_subscription_from_checkout(checkout.internal_id, sub).await?;
            }
            // If no subscription, add extra credits for the one-time purchase
            None => {
                for item in line_items.unwrap_or_default().data {
                    let price_id = match item.price {
                        Some(price) => price.id,
                        None => continue,
                    };
                    add_credits_to_plan(checkout.internal_id, price_id, item.quantity).await?;
                }
            }
        };
    } else {
        return Err(LibError::invalid(
            "Checkout session not completed",
            anyhow!("Checkout session not completed: {status:?}"),
        ));
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

    let price_id = stripe::PriceId::from_str(&plan.price_id).map_err(|_| {
        LibError::invalid(
            "Invalid pricing plan configuration",
            anyhow!("Invalid Stripe price id: {}", plan.price_id),
        )
    })?;
    let expand: &[&str] = &["product"];
    let price = stripe::Price::retrieve(&client, &price_id, expand)
        .await
        .map_err(|e| {
            LibError::upstream(
                "Failed to retrieve product information",
                anyhow!("Stripe price retrieve failed: {e}"),
            )
        })?;

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
            LibError::upstream(
                "Failed to create checkout session",
                anyhow!(
                    "create_checkout_cart error creating checkout session: {:?}",
                    err
                ),
            )
        })?;
    let secret = stripe_session.client_secret.ok_or_else(|| {
        tracing::error!("Error fetching client secret");
        LibError::upstream(
            "Failed to create checkout session",
            anyhow!("create_checkout_cart error fetching client secret"),
        )
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
    let sub_id = sub.subscription_id.ok_or_else(|| {
        LibError::not_found(
            "No subscription found",
            anyhow!("No subscription for id {}", internal_id),
        )
    })?;

    let sub_id = SubscriptionId::from_str(&sub_id).map_err(|err| {
        tracing::error!("Error parsing subscription ID: {}", err);
        LibError::invalid(
            "Invalid subscription ID",
            anyhow!(
                "deactivate_subscription error parsing subscription ID: {}",
                err
            ),
        )
    })?;
    let client = stripe_client_from_env()?;

    stripe::Subscription::cancel(&client, &sub_id, CancelSubscription::default())
        .await
        .map_err(|err| {
            tracing::error!("Error cancelling subscription: {}", err);
            LibError::upstream(
                "Error cancelling subscription",
                anyhow!("Error cancelling subscription"),
            )
        })?;

    cancel_subscription(internal_id).await?;
    Ok(())
}

fn unix_seconds_to_systemtime(ts: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(ts, 0).single().unwrap()
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

pub async fn subscription_updated<F, Fut, G, GFut>(
    schedule: SubscriptionSchedule,
    get_billing_link: F,
    update_subscription: G,
) -> Result<()>
where
    F: FnOnce(String) -> Fut,
    Fut: Future<Output = Option<Uuid>>,
    G: FnOnce(Uuid, SubscriptionStateUpdate) -> GFut,
    GFut: Future<Output = Result<()>>,
{
    // 1) Identify the customer on the schedule
    let customer_id = match schedule.customer {
        Expandable::Id(id) => id,
        Expandable::Object(cust) => cust.id.clone(),
    };

    // 2) Map to internal_id (if we don't know this customer, do nothing)
    let internal_id = match get_billing_link(customer_id.to_string()).await {
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
        .map_err(|e| {
            LibError::upstream(
                "Failed to retrieve subscription information",
                anyhow!("subscription_updated stripe subscription retrieve failed: {e}"),
            )
        })?;

    // 4) Compute remaining access window (or 0 if ended)
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
    update_subscription(internal_id, update).await?;
    Ok(())
}

pub async fn handle_stripe_event<F, Fut, G, GFut, H, HFut, I, IFut, J, JFut>(
    event: Event,
    get_billing_link: F,
    inactivate_subscription: G,
    get_subscription: H,
    cancel_subscription: I,
    update_subscription: J,
) -> Result<()>
where
    F: FnOnce(String) -> Fut,
    Fut: Future<Output = Option<Uuid>>,
    G: Fn(String, bool) -> GFut,
    GFut: Future<Output = Result<()>>,
    H: FnOnce(Uuid) -> HFut,
    HFut: Future<Output = Result<SubscriptionState>>,
    I: FnOnce(Uuid) -> IFut,
    IFut: Future<Output = Result<()>>,
    J: FnOnce(Uuid, SubscriptionStateUpdate) -> JFut,
    JFut: Future<Output = Result<()>>,
{
    match event.type_ {
        EventType::SubscriptionScheduleAborted | EventType::SubscriptionScheduleCompleted => {
            let sub = match event.data.object {
                EventObject::SubscriptionSchedule(sub) => sub,
                _ => return Ok(()),
            };
            let sub_id = sub.id.as_str().to_owned();
            inactivate_subscription(sub_id, true).await?;
        }
        EventType::SubscriptionScheduleCanceled => {
            let sub = match event.data.object {
                EventObject::SubscriptionSchedule(sub) => sub,
                _ => return Ok(()),
            };
            let internal_id = get_billing_link(sub.customer.id().as_str().to_owned())
                .await
                .ok_or_else(|| {
                    tracing::error!(
                        "Error fetching billing link for subscription {}",
                        sub.customer.id()
                    );
                    LibError::upstream(
                        "Error fetching billing link",
                        anyhow!("handle_stripe_event error fetching billing link"),
                    )
                })?;
            deactivate_subscription(internal_id, get_subscription, cancel_subscription).await?;
        }
        EventType::SubscriptionScheduleCreated => {
            let sub = match event.data.object {
                EventObject::SubscriptionSchedule(sub) => sub,
                _ => return Ok(()),
            };
            subscription_updated(sub, get_billing_link, update_subscription).await?;
        }
        EventType::SubscriptionScheduleUpdated => {
            let sub = match event.data.object {
                EventObject::SubscriptionSchedule(sub) => sub,
                _ => return Ok(()),
            };
            subscription_updated(sub, get_billing_link, update_subscription).await?;
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
