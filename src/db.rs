use std::sync::Arc;

use anyhow::anyhow;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use sqlx::migrate::{MigrateError, Migrator};
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

use crate::error::{LibError, Result};
use crate::models::{self, ApiProduct, BoxFut, FinalizeCheckout, stripe_client_from_env};
use crate::tables::{
    BillingLink, CheckoutSession, PricingPlan, SubscriptionState, SubscriptionStateUpdate,
    SubscriptionType,
};

pub static MIGRATOR: Lazy<Migrator> = Lazy::new(|| {
    let mut m = sqlx::migrate!("./migrations");
    m.set_ignore_missing(true);
    m
});

pub async fn create_stripe_tables(pool: &PgPool) -> Result<(), MigrateError> {
    MIGRATOR.run(pool).await
}

#[derive(Debug, Clone, FromRow)]
pub struct PriceRow {
    pub key: String,
    pub price_id: String,
}

impl PriceRow {
    pub fn new(key: &str, price_id: &str) -> Self {
        Self {
            key: key.to_string(),
            price_id: price_id.to_string(),
        }
    }

    pub fn table_name() -> &'static str {
        "stripe.prices"
    }

    pub async fn insert(pool: &PgPool, row: &PriceRow) -> Result<(), sqlx::Error> {
        sqlx::query(&format!(
            r#"
            INSERT INTO {} (key, price_id)
            VALUES ($1, $2)
            ON CONFLICT (key) DO UPDATE SET price_id = EXCLUDED.price_id
            "#,
            Self::table_name()
        ))
        .bind(&row.key)
        .bind(&row.price_id)
        .execute(pool)
        .await?;

        Ok(())
    }

    pub async fn get(pool: &PgPool, key: &str) -> Result<Option<Self>, sqlx::Error> {
        sqlx::query_as::<_, PriceRow>(&format!(
            r#"
            SELECT
                key,
                price_id
            FROM {}
            WHERE key = $1
            LIMIT 1
            "#,
            Self::table_name()
        ))
        .bind(key)
        .fetch_optional(pool)
        .await
    }

    pub async fn get_all(pool: &PgPool) -> Result<Vec<Self>, sqlx::Error> {
        sqlx::query_as::<_, PriceRow>(&format!(
            r#"
            SELECT
                key,
                price_id
            FROM {}
            "#,
            Self::table_name()
        ))
        .fetch_all(pool)
        .await
    }
}

pub async fn get_product(pool: Arc<PgPool>, key: &str) -> Result<ApiProduct> {
    let fetch_plan = |k: &str| -> BoxFut<Result<PricingPlan>> {
        let pool = pool.clone();
        let k = k.to_string();
        Box::pin(async move {
            let opt = PriceRow::get(&pool, &k).await.map_err(|_| {
                LibError::database(
                    "Failed to fetch price",
                    anyhow!("db::get_product missing price {}", k),
                )
            })?;
            let row = opt.ok_or_else(|| {
                LibError::not_found(
                    "Price not found",
                    anyhow!("db::get_product price not found for key {}", k),
                )
            })?;
            Ok(PricingPlan::new(&row.key, &row.price_id))
        })
    };

    models::get_product(key, &fetch_plan).await
}

pub async fn get_products(pool: Arc<PgPool>) -> Result<Vec<ApiProduct>> {
    let fetch_all = async || -> Result<Vec<PricingPlan>> {
        let rows = PriceRow::get_all(&pool).await.map_err(|e| {
            LibError::database(
                "Failed to fetch prices",
                anyhow!("db::get_products failed to fetch prices: {}", e),
            )
        })?;
        let plans = rows
            .into_iter()
            .map(|row| PricingPlan::new(&row.key, &row.price_id))
            .collect();
        Ok(plans)
    };

    let fetch_plan = |k: &str| -> BoxFut<Result<PricingPlan>> {
        let pool = pool.clone();
        let k = k.to_string();
        Box::pin(async move {
            let opt = PriceRow::get(&pool, &k).await.map_err(|_| {
                LibError::database(
                    "Failed to fetch price",
                    anyhow!("db::get_products missing price {k}"),
                )
            })?;
            let row = opt.ok_or_else(|| {
                LibError::not_found("Price not found", anyhow!("Price not found for key {k}"))
            })?;
            Ok(PricingPlan::new(&row.key, &row.price_id))
        })
    };

    models::get_products(&fetch_all, &fetch_plan).await
}

#[derive(PartialEq, Clone, Debug, Deserialize, Serialize, FromRow)]
pub struct BillingLinkRow {
    pub customer_id: String,
    pub internal_id: Uuid,
    pub created: chrono::NaiveDateTime,
}

impl BillingLinkRow {
    pub fn new(customer_id: &str, internal_id: Uuid) -> Self {
        Self {
            customer_id: customer_id.to_string(),
            internal_id,
            created: chrono::Utc::now().naive_utc(),
        }
    }

    pub fn table_name() -> &'static str {
        "stripe.customers"
    }

    pub async fn insert(pool: &PgPool, row: &BillingLinkRow) -> Result<(), sqlx::Error> {
        sqlx::query(&format!(
            r#"
            INSERT INTO {} (customer_id, internal_id, created)
            VALUES ($1, $2, $3)
            ON CONFLICT (internal_id) DO UPDATE SET customer_id = EXCLUDED.customer_id, created = EXCLUDED.created
            "#,
            Self::table_name()
        ))
        .bind(&row.customer_id)
        .bind(row.internal_id)
        .bind(row.created)
        .execute(pool)
        .await?;

        Ok(())
    }

    pub async fn get_by_customer_id(
        pool: &PgPool,
        customer_id: &str,
    ) -> Result<Option<Self>, sqlx::Error> {
        sqlx::query_as::<_, BillingLinkRow>(&format!(
            r#"
            SELECT
                customer_id,
                internal_id,
                created
            FROM {}
            WHERE customer_id = $1
            LIMIT 1
            "#,
            Self::table_name()
        ))
        .bind(customer_id)
        .fetch_optional(pool)
        .await
    }

    pub async fn get_by_internal_id(
        pool: &PgPool,
        internal_id: Uuid,
    ) -> Result<Option<Self>, sqlx::Error> {
        sqlx::query_as::<_, BillingLinkRow>(&format!(
            r#"
            SELECT
                customer_id,
                internal_id,
                created
            FROM {}
            WHERE internal_id = $1
            LIMIT 1
            "#,
            Self::table_name()
        ))
        .bind(internal_id)
        .fetch_optional(pool)
        .await
    }
}

#[derive(Debug, Clone, FromRow)]
pub struct CheckoutSessionRow {
    reference_id: Uuid,
    internal_id: Uuid,
    created: chrono::NaiveDateTime,
    expires: chrono::NaiveDateTime,
}

impl CheckoutSessionRow {
    pub fn new(reference_id: Uuid, internal_id: Uuid) -> Self {
        let now = chrono::Utc::now().naive_utc();
        let expires = now + chrono::Duration::minutes(30); // Example duration

        Self {
            reference_id,
            internal_id,
            created: now,
            expires,
        }
    }

    pub fn table_name() -> &'static str {
        "stripe.checkout_sessions"
    }

    pub async fn insert(pool: &PgPool, row: &CheckoutSessionRow) -> Result<(), sqlx::Error> {
        sqlx::query(&format!(
            r#"
            INSERT INTO {} (reference_id, internal_id, created, expires)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (reference_id) DO UPDATE SET internal_id = EXCLUDED.internal_id, created = EXCLUDED.created, expires = EXCLUDED.expires
            "#,
            Self::table_name()
        ))
        .bind(row.reference_id)
        .bind(row.internal_id)
        .bind(row.created)
        .bind(row.expires)
        .execute(pool)
        .await?;

        Ok(())
    }

    pub async fn get_by_reference_id(
        pool: &PgPool,
        reference_id: Uuid,
    ) -> Result<Option<Self>, sqlx::Error> {
        sqlx::query_as::<_, CheckoutSessionRow>(&format!(
            r#"
            SELECT
                reference_id,
                internal_id,
                created,
                expires
            FROM {}
            WHERE reference_id = $1
            LIMIT 1
            "#,
            Self::table_name()
        ))
        .bind(reference_id)
        .fetch_optional(pool)
        .await
    }

    pub async fn delete_by_reference_id(
        pool: &PgPool,
        reference_id: Uuid,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(&format!(
            r#"
            DELETE FROM {} WHERE reference_id = $1
            "#,
            Self::table_name()
        ))
        .bind(reference_id)
        .execute(pool)
        .await?;

        Ok(())
    }
}

pub async fn create_checkout_cart(
    pool: Arc<PgPool>,
    internal_id: Uuid,
    base_url: &str,
    stripe_return_uri: &str,
    key: &str,
    quantity: u64,
) -> Result<String> {
    let get_customer_id = async |internal_id: Uuid| -> Option<String> {
        BillingLinkRow::get_by_internal_id(&pool, internal_id)
            .await
            .map(|opt| opt.map(|row| row.customer_id))
            .ok()
            .flatten()
    };

    let create_checkout_session = async |session: CheckoutSession| -> () {
        let row = CheckoutSessionRow::new(session.reference_id, session.internal_id);
        if let Err(e) = CheckoutSessionRow::insert(&pool, &row).await {
            tracing::error!("Failed to create checkout session: {}", e);
        }
    };

    let PriceRow { key, price_id } = PriceRow::get(&pool, key)
        .await
        .map_err(|e| {
            LibError::database(
                "Failed to get price",
                anyhow!("db::create_checkout_cart failed to get price: {}", e),
            )
        })?
        .ok_or_else(|| {
            LibError::not_found(
                "Price not found",
                anyhow!("db::create_checkout_cart price not found for key {}", key),
            )
        })?;
    let plan = PricingPlan { key, price_id };

    models::create_checkout_cart(
        internal_id,
        base_url,
        stripe_return_uri,
        &plan,
        quantity,
        get_customer_id,
        create_checkout_session,
    )
    .await
}

pub async fn checkout_status(
    pool: Arc<PgPool>,
    params: FinalizeCheckout,
) -> Result<serde_json::Value> {
    let get_checkout_session = async |reference_id: Uuid| -> Result<CheckoutSession> {
        let row = CheckoutSessionRow::get_by_reference_id(&pool, reference_id)
            .await
            .map_err(|e| {
                LibError::database(
                    "Failed to get checkout session",
                    anyhow!("db::checkout_status failed to get checkout session: {}", e),
                )
            })?
            .ok_or_else(|| {
                LibError::not_found(
                    "Checkout session not found",
                    anyhow!("db::checkout_status session not found"),
                )
            })?;
        Ok(CheckoutSession {
            reference_id: row.reference_id,
            internal_id: row.internal_id,
            created: row.created,
            expires: row.expires,
        })
    };

    let upsert_billing_link = async |link: BillingLink| -> Result<()> {
        let link = BillingLinkRow::new(&link.customer_id, link.internal_id);
        BillingLinkRow::insert(&pool, &link).await.map_err(|e| {
            LibError::database(
                "Failed to upsert billing link",
                anyhow!("db::checkout_status failed to upsert billing link: {e}"),
            )
        })
    };

    let update_subscription_from_checkout = async |internal_id: Uuid,
                                                   sub: stripe::Subscription|
           -> Result<()> {
        let now = chrono::Utc::now().naive_utc();
        let current_period =
            chrono::DateTime::<chrono::Utc>::from_timestamp(sub.current_period_end, 0)
                .map(|dt| dt.naive_utc());
        let row = SubscriptionRow {
            internal_id,
            created: now,
            updated: now,
            subscription_id: Some(sub.id.to_string()),
            subscription_type: serde_json::to_string(&SubscriptionType::Paid).expect("serialize"),
            seats: 1,
            is_active: sub.status == stripe::SubscriptionStatus::Active,
            current_period_timestamp: current_period,
            cancel_at_period_end: sub.cancel_at_period_end,
            last_payment_failed: false,
            is_auto_billing: true,
        };
        SubscriptionRow::insert(&pool, &row).await.map_err(|e| {
            LibError::database(
                "Failed to update subscription",
                anyhow!("db::checkout_status failed to update subscription: {e}"),
            )
        })
    };

    let add_credits_to_plan =
        async |internal_id: Uuid, price_id: stripe::PriceId, quantity: Option<u64>| -> Result<()> {
            let client = stripe_client_from_env()?;
            let price = stripe::Price::retrieve(&client, &price_id, &["product"])
                .await
                .map_err(|e| {
                    LibError::upstream(
                        "Failed to retrieve price from Stripe",
                        anyhow!("db::checkout_status failed to retrieve price from Stripe: {e}"),
                    )
                })?;
            let credits = models::credits_from_price(&client, &price).await;
            match credits {
                Some(credits) => {
                    let total_credits = credits * quantity.unwrap_or(1) as i32;
                    CreditRow::add_credits(&pool, internal_id, total_credits)
                        .await
                        .map_err(|e| {
                            LibError::database(
                                "Failed to add credits",
                                anyhow!("db::checkout_status failed to add credits: {e}"),
                            )
                        })?;
                }
                _ => {}
            }
            Ok(())
        };

    let delete_checkout_session = async |reference_id: Uuid| -> Result<()> {
        CheckoutSessionRow::delete_by_reference_id(&pool, reference_id)
            .await
            .map_err(|e| {
                LibError::database(
                    "Failed to delete checkout session",
                    anyhow!("Failed to delete checkout session: {e}"),
                )
            })
    };

    models::checkout_status(
        params,
        &get_checkout_session,
        &upsert_billing_link,
        &update_subscription_from_checkout,
        &add_credits_to_plan,
        &delete_checkout_session,
    )
    .await
}

#[derive(PartialEq, Clone, Debug, Deserialize, Serialize, FromRow)]
pub struct CreditRow {
    pub internal_id: Uuid,
    pub credits: i32,
    pub updated: chrono::NaiveDateTime,
}

impl CreditRow {
    pub fn new(internal_id: Uuid, credits: i32) -> Self {
        Self {
            internal_id,
            credits,
            updated: chrono::Utc::now().naive_utc(),
        }
    }

    pub fn table_name() -> &'static str {
        "stripe.credits"
    }

    pub async fn add_credits(
        pool: &PgPool,
        internal_id: Uuid,
        additional_credits: i32,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(&format!(
            r#"
            INSERT INTO {} (internal_id, credits, updated)
            VALUES ($1, $2, $3)
            ON CONFLICT (internal_id) DO UPDATE SET credits = stripe.credits.credits + EXCLUDED.credits, updated = EXCLUDED.updated
            "#,
            Self::table_name()
        ))
        .bind(internal_id)
        .bind(additional_credits)
        .bind(chrono::Utc::now().naive_utc())
        .execute(pool)
        .await?;

        Ok(())
    }

    pub async fn get_credits(pool: &PgPool, internal_id: Uuid) -> Result<Option<i32>, sqlx::Error> {
        let row = sqlx::query_as::<_, CreditRow>(&format!(
            r#"
            SELECT
                internal_id,
                credits,
                updated
            FROM {}
            WHERE internal_id = $1
            LIMIT 1
            "#,
            Self::table_name()
        ))
        .bind(internal_id)
        .fetch_optional(pool)
        .await?;

        Ok(row.map(|r| r.credits))
    }

    pub async fn sub_credits(
        pool: &PgPool,
        internal_id: Uuid,
        subtracted_credits: i32,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(&format!(
            r#"
            UPDATE {} SET credits = credits - $2, updated = $3
            WHERE internal_id = $1 AND credits >= $2
            "#,
            Self::table_name()
        ))
        .bind(internal_id)
        .bind(subtracted_credits)
        .bind(chrono::Utc::now().naive_utc())
        .execute(pool)
        .await?;

        Ok(())
    }
}

#[derive(PartialEq, Clone, Debug, Deserialize, Serialize, FromRow)]
pub struct SubscriptionRow {
    pub internal_id: Uuid,
    pub created: chrono::NaiveDateTime,
    pub updated: chrono::NaiveDateTime,
    pub subscription_id: Option<String>,
    pub subscription_type: String,
    pub seats: i32,
    pub is_active: bool,
    pub current_period_timestamp: Option<chrono::NaiveDateTime>,
    pub cancel_at_period_end: bool,
    pub last_payment_failed: bool,
    pub is_auto_billing: bool,
}

impl SubscriptionRow {
    pub fn new(internal_id: Uuid, subscription_type: &str, seats: i32) -> Self {
        let now = chrono::Utc::now().naive_utc();

        Self {
            internal_id,
            created: now,
            updated: now,
            subscription_id: None,
            subscription_type: subscription_type.to_string(),
            seats,
            is_active: true,
            current_period_timestamp: None,
            cancel_at_period_end: false,
            last_payment_failed: false,
            is_auto_billing: true,
        }
    }

    pub fn table_name() -> &'static str {
        "stripe.subscriptions"
    }

    pub async fn insert(pool: &PgPool, row: &SubscriptionRow) -> Result<(), sqlx::Error> {
        sqlx::query(&format!(
            r#"
            INSERT INTO {} (internal_id, created, updated, subscription_id, subscription_type, seats, is_active, current_period_timestamp, cancel_at_period_end, last_payment_failed, is_auto_billing)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (internal_id) DO UPDATE SET
                updated = EXCLUDED.updated,
                subscription_id = EXCLUDED.subscription_id,
                subscription_type = EXCLUDED.subscription_type,
                seats = EXCLUDED.seats,
                is_active = EXCLUDED.is_active,
                current_period_timestamp = EXCLUDED.current_period_timestamp,
                cancel_at_period_end = EXCLUDED.cancel_at_period_end,
                last_payment_failed = EXCLUDED.last_payment_failed,
                is_auto_billing = EXCLUDED.is_auto_billing
            "#,
            Self::table_name()
        ))
        .bind(row.internal_id)
        .bind(row.created)
        .bind(row.updated)
        .bind(&row.subscription_id)
        .bind(&row.subscription_type)
        .bind(row.seats)
        .bind(row.is_active)
        .bind(&row.current_period_timestamp)
        .bind(row.cancel_at_period_end)
        .bind(row.last_payment_failed)
        .bind(row.is_auto_billing)
        .execute(pool)
        .await?;

        Ok(())
    }

    pub async fn inactivate_by_sub_id(
        pool: &PgPool,
        subscription_id: &str,
        last_payment_failed: bool,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(&format!(
            r#"
            UPDATE {} SET
                subscription_id = NULL,
                is_active = FALSE,
                seats = 0,
                last_payment_failed = $2,
                updated = $3
            WHERE subscription_id = $1
            "#,
            Self::table_name()
        ))
        .bind(subscription_id)
        .bind(last_payment_failed)
        .bind(chrono::Utc::now().naive_utc())
        .execute(pool)
        .await?;

        Ok(())
    }

    pub async fn cancel_by_internal_id(
        pool: &PgPool,
        internal_id: Uuid,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(&format!(
            r#"
            UPDATE {} SET cancel_at_period_end = TRUE, is_auto_billing = FALSE, updated = $2
            WHERE internal_id = $1
            "#,
            Self::table_name()
        ))
        .bind(internal_id)
        .bind(chrono::Utc::now().naive_utc())
        .execute(pool)
        .await?;

        Ok(())
    }

    pub async fn get_by_internal_id(
        pool: &PgPool,
        internal_id: Uuid,
    ) -> Result<Option<Self>, sqlx::Error> {
        sqlx::query_as::<_, SubscriptionRow>(&format!(
            r#"
            SELECT
                internal_id,
                created,
                updated,
                subscription_id,
                subscription_type,
                seats,
                is_active,
                current_period_timestamp,
                cancel_at_period_end,
                last_payment_failed,
                is_auto_billing
            FROM {}
            WHERE internal_id = $1
            LIMIT 1
            "#,
            Self::table_name()
        ))
        .bind(internal_id)
        .fetch_optional(pool)
        .await
    }

    pub async fn get_by_subscription_id(
        pool: &PgPool,
        subscription_id: &str,
    ) -> Result<Option<Self>, sqlx::Error> {
        sqlx::query_as::<_, SubscriptionRow>(&format!(
            r#"
            SELECT
                internal_id,
                created,
                updated,
                subscription_id,
                subscription_type,
                seats,
                is_active,
                current_period_timestamp,
                cancel_at_period_end,
                last_payment_failed,
                is_auto_billing
            FROM {}
            WHERE subscription_id = $1
            LIMIT 1
            "#,
            Self::table_name()
        ))
        .bind(subscription_id)
        .fetch_optional(pool)
        .await
    }

    pub async fn update_by_internal_id(
        pool: &PgPool,
        internal_id: Uuid,
        update: SubscriptionStateUpdate,
    ) -> Result<(), sqlx::Error> {
        let SubscriptionStateUpdate {
            subscription_id,
            subscription_type,
            cancel_at_period_end,
            current_period_end,
            is_auto_billing,
            seats,
        } = update;
        let mut query = format!("UPDATE {} SET ", Self::table_name());

        let mut sets = vec![];

        if let Some(sub_id) = subscription_id {
            sets.push(format!("subscription_id = '{}'", sub_id));
        }
        sets.push(format!(
            "subscription_type = '{}'",
            serde_json::to_string(&subscription_type).unwrap()
        ));
        sets.push(format!("cancel_at_period_end = {}", cancel_at_period_end));
        if let Some(period_end) = current_period_end {
            sets.push(format!("current_period_timestamp = '{}'", period_end));
        }
        sets.push(format!("is_auto_billing = {}", is_auto_billing));
        if let Some(seat_count) = seats {
            sets.push(format!("seats = {}", seat_count));
        }
        sets.push(format!("updated = '{}'", chrono::Utc::now().naive_utc()));

        query.push_str(&sets.join(", "));
        query.push_str(" WHERE internal_id = $1");

        sqlx::query(&query).bind(internal_id).execute(pool).await?;

        Ok(())
    }
}

fn sub_row_to_state(row: SubscriptionRow) -> Result<SubscriptionState> {
    Ok(SubscriptionState {
        internal_id: row.internal_id,
        created: row.created,
        updated: row.updated,
        subscription_id: row.subscription_id,
        subscription_type: serde_json::from_str(&row.subscription_type).map_err(|e| {
            LibError::database(
                "Failed to parse subscription type",
                anyhow!("db::sub_row_to_state failed to parse subscription type: {e}"),
            )
        })?,
        seats: Some(row.seats as i32),
        is_active: row.is_active,
        current_period_end: row.current_period_timestamp,
        cancel_at_period_end: row.cancel_at_period_end,
        last_payment_failed: row.last_payment_failed,
        is_auto_billing: row.is_auto_billing,
    })
}

pub async fn get_subscription(pool: Arc<PgPool>, internal_id: Uuid) -> Result<SubscriptionState> {
    let row = SubscriptionRow::get_by_internal_id(&pool, internal_id)
        .await
        .map_err(|e| {
            LibError::database(
                "Failed to get subscription",
                anyhow!("db::get_subscription failed to get subscription: {e}"),
            )
        })?
        .ok_or_else(|| {
            LibError::not_found(
                "Subscription not found",
                anyhow!("db::get_subscription subscription not found"),
            )
        })?;
    sub_row_to_state(row)
}

pub async fn deactivate_subscription(pool: Arc<PgPool>, internal_id: Uuid) -> Result<()> {
    let get_subscription = async |internal_id: Uuid| -> Result<SubscriptionState> {
        let row = SubscriptionRow::get_by_internal_id(&pool, internal_id)
            .await
            .map_err(|e| {
                LibError::database(
                    "Failed to get subscription",
                    anyhow!("Failed to get subscription: {e}"),
                )
            })?
            .ok_or_else(|| {
                LibError::not_found(
                    "Subscription not found",
                    anyhow!("db::get_subscription subscription not found"),
                )
            })?;
        Ok(sub_row_to_state(row)?)
    };

    let cancel_subscription = async |internal_id: Uuid| -> Result<()> {
        SubscriptionRow::cancel_by_internal_id(&pool, internal_id)
            .await
            .map_err(|e| {
                LibError::database(
                    "Failed to update subscription",
                    anyhow!("db::get_subscription failed to update subscription: {e}"),
                )
            })
    };

    models::deactivate_subscription(internal_id, &get_subscription, &cancel_subscription).await
}

pub async fn handle_stripe_event(pool: Arc<PgPool>, event: stripe::Event) -> Result<()> {
    let get_billing_link = async |customer_id: String| -> Option<Uuid> {
        let row = BillingLinkRow::get_by_customer_id(&pool, &customer_id)
            .await
            .ok()
            .flatten()?;
        Some(row.internal_id)
    };

    let inactivate_subscription =
        async |subscription_id: String, last_payment_failed: bool| -> Result<()> {
            SubscriptionRow::inactivate_by_sub_id(&pool, &subscription_id, last_payment_failed)
                .await
                .map_err(|e| {
                    LibError::database(
                        "Failed to update subscription",
                        anyhow!("db::handle_stripe_event failed to update subscription: {e}"),
                    )
                })
        };

    let get_subscription = async |internal_id: Uuid| -> Result<SubscriptionState> {
        let row = SubscriptionRow::get_by_internal_id(&pool, internal_id)
            .await
            .map_err(|e| {
                LibError::database(
                    "Failed to get subscription",
                    anyhow!("db::handle_stripe_event failed to get subscription: {e}"),
                )
            })?
            .ok_or_else(|| {
                LibError::not_found(
                    "Subscription not found",
                    anyhow!("db::handle_stripe_event subscription not found"),
                )
            })?;
        Ok(sub_row_to_state(row)?)
    };

    let cancel_subscription = async |internal_id: Uuid| -> Result<()> {
        SubscriptionRow::cancel_by_internal_id(&pool, internal_id)
            .await
            .map_err(|e| {
                LibError::database(
                    "Failed to update subscription",
                    anyhow!("db::handle_stripe_event failed to update subscription: {e}"),
                )
            })
    };

    let update_subscription =
        async |internal_id: Uuid, update: SubscriptionStateUpdate| -> Result<()> {
            SubscriptionRow::update_by_internal_id(&pool, internal_id, update)
                .await
                .map_err(|e| {
                    LibError::database(
                        "Failed to update subscription",
                        anyhow!("db::handle_stripe_event failed to update subscription: {e}"),
                    )
                })
        };

    models::handle_stripe_event(
        event,
        get_billing_link,
        inactivate_subscription,
        get_subscription,
        cancel_subscription,
        update_subscription,
    )
    .await
}
