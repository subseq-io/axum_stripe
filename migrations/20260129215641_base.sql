CREATE SCHEMA IF NOT EXISTS stripe;

CREATE TABLE IF NOT EXISTS stripe.prices (
    key TEXT PRIMARY KEY,
    price_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stripe.customers (
    internal_id UUID PRIMARY KEY,
    customer_id TEXT NOT NULL,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stripe_customers_customer_id
    ON stripe.customers (customer_id);

CREATE TABLE IF NOT EXISTS stripe.checkout_sessions (
    reference_id UUID PRIMARY KEY,
    internal_id UUID NOT NULL,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS stripe.subscriptions (
    internal_id UUID PRIMARY KEY,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    subscription_id TEXT,
    subscription_type TEXT NOT NULL,
    seats INTEGER,
    is_active BOOLEAN DEFAULT TRUE,
    current_period_timestamp TIMESTAMP,
    cancel_at_period_end BOOLEAN DEFAULT FALSE,
    last_payment_failed BOOLEAN DEFAULT FALSE,
    is_auto_billing BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS stripe.credits (
    internal_id UUID PRIMARY KEY,
    credits INTEGER NOT NULL,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stripe_subscriptions_subscription_id
    ON stripe.subscriptions (subscription_id);
