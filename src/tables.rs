use std::str::FromStr;

use chrono::{Duration, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

fn checkout_session_duration() -> chrono::Duration {
    match std::env::var("STRIPE_CHECKOUT_SESSION_DURATION_MINUTES") {
        Ok(val) => {
            if let Ok(minutes) = val.parse::<i64>() {
                return chrono::Duration::minutes(minutes);
            }
            chrono::Duration::minutes(15)
        }
        Err(_) => chrono::Duration::minutes(15),
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PricingPlan {
    pub key: String,
    pub price_id: String,
}

impl PricingPlan {
    pub fn new(key: &str, price_id: &str) -> Self {
        Self {
            key: key.to_string(),
            price_id: price_id.to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SubscriptionType {
    Trial,
    Paid,
    Internal,
    Custom,
}

impl FromStr for SubscriptionType {
    type Err = serde_json::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s)
    }
}

impl ToString for SubscriptionType {
    fn to_string(&self) -> String {
        serde_json::to_string(&self).expect("serialize")
    }
}

/// Link between the internal ID and the stripe customer ID.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct BillingLink {
    pub customer_id: String,
    pub internal_id: Uuid,
    pub created: NaiveDateTime,
}

impl BillingLink {
    pub fn new(customer_id: &str, internal_id: Uuid) -> Self {
        Self {
            customer_id: customer_id.to_string(),
            internal_id,
            created: Utc::now().naive_utc(),
        }
    }
}

#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct CheckoutSession {
    pub reference_id: Uuid,
    pub internal_id: Uuid,
    pub created: NaiveDateTime,
    pub expires: NaiveDateTime,
}

impl CheckoutSession {
    pub fn new(reference_id: Uuid, internal_id: Uuid) -> Self {
        let now = Utc::now().naive_utc();
        let expires = now + checkout_session_duration();

        Self {
            reference_id,
            internal_id,
            created: now,
            expires,
        }
    }
}

#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct SubscriptionState {
    pub internal_id: Uuid,
    pub created: NaiveDateTime,
    pub updated: NaiveDateTime,
    pub subscription_id: Option<String>,
    pub subscription_type: SubscriptionType,
    pub seats: Option<i32>,
    pub is_active: bool,
    pub current_period_end: Option<NaiveDateTime>,
    pub cancel_at_period_end: bool,
    pub last_payment_failed: bool,
    pub is_auto_billing: bool,
}

impl SubscriptionState {
    pub async fn new(internal_id: Uuid, seats: Option<i32>, trial_period: Duration) -> Self {
        let now = Utc::now().naive_utc();
        let trial_end = now + trial_period;

        Self {
            internal_id,
            created: now,
            updated: now,
            subscription_id: None,
            subscription_type: SubscriptionType::Trial,
            seats,
            is_active: true,
            current_period_end: Some(trial_end),
            cancel_at_period_end: true,
            last_payment_failed: false,
            is_auto_billing: false,
        }
    }
}

#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct SubscriptionStateUpdate {
    pub subscription_id: Option<String>,
    pub subscription_type: SubscriptionType,
    pub cancel_at_period_end: bool,
    pub current_period_end: Option<NaiveDateTime>,
    pub is_auto_billing: bool,
    pub seats: Option<u64>,
}
