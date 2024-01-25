use std::sync::Arc;

use axum::{
    extract::{Query, State},
    response::IntoResponse,
    Json,
};
use engine_v2::EngineEnv;
use gateway_v2::{Gateway, GatewayEnv};
use http::HeaderMap;
use tower_http::cors::CorsLayer;

use crate::dev::handle_engine_request;

pub(super) async fn run(port: u16, expose: bool, config: engine_v2::VersionedConfig) -> Result<(), crate::Error> {
    let gateway = Arc::new(Gateway::new(
        config.into_latest().into(),
        EngineEnv {
            fetcher: runtime_local::NativeFetcher::runtime_fetcher(),
        },
        GatewayEnv {
            kv: runtime_local::InMemoryKvStore::runtime(),
            cache: runtime_local::InMemoryCache::runtime(runtime::cache::GlobalCacheConfig {
                common_cache_tags: vec![],
                enabled: true,
                subdomain: "localhost".to_string(),
            }),
        },
    ));

    let app = axum::Router::new()
        .route("/graphql", axum::routing::get(engine_get).post(engine_post))
        .layer(CorsLayer::permissive())
        .with_state(gateway);

    let host = if expose {
        format!("0.0.0.0:{port}")
    } else {
        format!("127.0.0.1:{port}")
    };
    let address: std::net::SocketAddr = host.parse().expect("we just defined it above, it _must work_");

    let listener = tokio::net::TcpListener::bind(&address).await.unwrap();
    axum::serve(listener, app)
        .await
        .map_err(|error| crate::Error::internal(error.to_string()))?;

    Ok(())
}

async fn engine_get(
    Query(request): Query<engine::Request>,
    headers: HeaderMap,
    State(gateway): State<Arc<Gateway>>,
) -> impl IntoResponse {
    handle_engine_request(request, gateway, headers).await
}

async fn engine_post(
    State(gateway): State<Arc<Gateway>>,
    headers: HeaderMap,
    Json(request): Json<engine::Request>,
) -> impl IntoResponse {
    handle_engine_request(request, gateway, headers).await
}
