use std::sync::Arc;

use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Json, Path, Query, Request, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use axum_auth_user::prelude::{AuthenticatedUser, ValidatesIdentity};
use serde::Deserialize;
use stripe::Webhook;

use crate::db;
use crate::error::{ErrorKind, LibError};
use crate::models::FinalizeCheckout;

#[derive(Debug)]
pub struct AppError(pub LibError);

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = match self.0.kind {
            ErrorKind::Database => StatusCode::INTERNAL_SERVER_ERROR,
            ErrorKind::InvalidInput => StatusCode::BAD_REQUEST,
            ErrorKind::Forbidden => StatusCode::FORBIDDEN,
            ErrorKind::NotFound => StatusCode::NOT_FOUND,
            ErrorKind::Unauthorized => StatusCode::UNAUTHORIZED,
            ErrorKind::Unknown => StatusCode::INTERNAL_SERVER_ERROR,
            ErrorKind::Upstream => StatusCode::INTERNAL_SERVER_ERROR,
        };
        tracing::error!(error = %self.0.source, kind = ?self.0.kind, "request failed");
        (status, self.0.public).into_response()
    }
}

impl From<LibError> for AppError {
    fn from(e: LibError) -> Self {
        Self(e)
    }
}

pub trait HasBaseUrl {
    fn base_url(&self) -> String;
}

pub trait HasPool {
    fn pool(&self) -> Arc<sqlx::PgPool>;
}

pub trait StripeApp: HasBaseUrl + HasPool + ValidatesIdentity {}

async fn get_product_handler<S>(
    State(app): State<S>,
    Path(product_id): Path<String>,
) -> Result<impl IntoResponse, AppError>
where
    S: StripeApp + Clone + Send + Sync + 'static,
{
    let product = db::get_product(app.pool(), &product_id).await?;
    Ok(Json(product))
}

async fn get_products_handler<S>(State(app): State<S>) -> Result<impl IntoResponse, AppError>
where
    S: StripeApp + Clone + Send + Sync + 'static,
{
    let products = db::get_products(app.pool()).await?;
    Ok(Json(products))
}

async fn stripe_checkout_status_handler<S>(
    State(app): State<S>,
    _auth_user: AuthenticatedUser,
    Query(params): Query<FinalizeCheckout>,
) -> Result<impl IntoResponse, AppError>
where
    S: StripeApp + Clone + Send + Sync + 'static,
{
    let status = db::checkout_status(app.pool(), params).await?;
    Ok(Json(status))
}

#[derive(Deserialize)]
pub struct SelectedPlan {
    pub key: String,
    pub quantity: u64,
    pub status_uri: String,
}

async fn stripe_checkout_cart_handler<S>(
    State(app): State<S>,
    auth_user: AuthenticatedUser,
    Query(params): Query<SelectedPlan>,
) -> Result<impl IntoResponse, AppError>
where
    S: StripeApp + Clone + Send + Sync + 'static,
{
    let internal_id = auth_user.id().0;
    let base_url = app.base_url();
    let SelectedPlan {
        key,
        quantity,
        status_uri,
    } = params;
    let session = db::create_checkout_cart(
        app.pool(),
        internal_id,
        &base_url,
        &status_uri,
        &key,
        quantity,
    )
    .await?;
    Ok(Json(serde_json::json!({"clientSecret": session})))
}

pub async fn stripe_webhook_handler<S>(
    State(app): State<S>,
    req: Request<Body>,
) -> Result<impl IntoResponse, StatusCode>
where
    S: StripeApp + Clone + Send + Sync + 'static,
{
    let (parts, body) = req.into_parts();
    let headers: HeaderMap = parts.headers;
    let body_bytes = to_bytes(body, 1024 * 1024)
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let body_str = std::str::from_utf8(&body_bytes).map_err(|_| StatusCode::BAD_REQUEST)?;

    let webhook_secret = match std::env::var("STRIPE_WEBHOOK_SECRET") {
        Ok(secret) => secret,
        Err(_) => {
            tracing::warn!("Stripe webhook secret not set, skipping webhook handling");
            return Ok(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };
    let sig = headers
        .get("Stripe-Signature")
        .ok_or(StatusCode::UNAUTHORIZED)?
        .to_str()
        .map_err(|_| StatusCode::UNAUTHORIZED)?;

    let event = Webhook::construct_event(&body_str, sig, &webhook_secret)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    db::handle_stripe_event(app.pool(), event)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(StatusCode::OK)
}

async fn stripe_get_subscription_handler<S>(
    auth_user: AuthenticatedUser,
    State(app): State<S>,
) -> Result<impl IntoResponse, AppError>
where
    S: StripeApp + Clone + Send + Sync + 'static,
{
    let info = db::get_subscription(app.pool(), auth_user.id().0).await?;
    Ok(Json(info))
}

async fn stripe_deactivate_subscription_handler<S>(
    auth_user: AuthenticatedUser,
    State(app): State<S>,
) -> Result<impl IntoResponse, AppError>
where
    S: StripeApp + Clone + Send + Sync + 'static,
{
    db::deactivate_subscription(app.pool(), auth_user.id().0).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub fn routes<S>() -> Router<S>
where
    S: StripeApp + Clone + Send + Sync + 'static,
{
    Router::new()
        .route(
            "/stripe/product/{product_id}",
            get(get_product_handler::<S>),
        )
        .route("/stripe/products", get(get_products_handler::<S>))
        .route(
            "/stripe/checkout/status",
            get(stripe_checkout_status_handler::<S>),
        )
        .route("/stripe/checkout", get(stripe_checkout_cart_handler::<S>))
        .route("/stripe/webhook", post(stripe_webhook_handler::<S>))
        .route(
            "/stripe/subscription",
            get(stripe_get_subscription_handler::<S>)
                .delete(stripe_deactivate_subscription_handler::<S>),
        )
}
