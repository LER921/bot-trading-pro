use anyhow::Result;
use axum::{extract::State, routing::get, Json, Router};
use config_loader::load_from_dir;
use control_plane::{ControlPlane, InMemoryControlPlane};
use domain::RuntimeState;
use serde::Serialize;
use std::net::SocketAddr;

#[derive(Clone)]
struct AppState {
    control_plane: InMemoryControlPlane,
}

#[derive(Serialize)]
struct HealthPayload {
    status: &'static str,
    mode: &'static str,
}

#[derive(Serialize)]
struct RuntimePayload {
    runtime_state: RuntimeState,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .json()
        .init();

    let config = load_from_dir("config")?;
    let state = AppState {
        control_plane: InMemoryControlPlane::new(RuntimeState::Paused),
    };

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/runtime", get(runtime_state))
        .with_state(state);

    let address: SocketAddr = config.dashboard.bind_address.parse()?;
    tracing::info!(
        bind = %address,
        "dashboardd started in read-mostly mode; operator commands must route through control_plane then risk"
    );

    let listener = tokio::net::TcpListener::bind(address).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn healthz() -> Json<HealthPayload> {
    Json(HealthPayload {
        status: "ok",
        mode: "read-mostly",
    })
}

async fn runtime_state(State(state): State<AppState>) -> Json<RuntimePayload> {
    Json(RuntimePayload {
        runtime_state: state.control_plane.current_state().await,
    })
}
