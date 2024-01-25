use std::sync::Arc;

use axum::{extract::State, response::IntoResponse, routing::post, Json};
use engine_v2::EngineEnv;
use federated_graph::FederatedGraph;
use futures_util::future::{join_all, BoxFuture};
use gateway_v2::{Gateway, GatewayEnv};
use http::HeaderMap;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use std::sync::OnceLock;

static CELL: OnceLock<bytes::Bytes> = OnceLock::new();

fn main() {
    let schema = std::fs::read_to_string("supergraph.graphql").unwrap();
    let FederatedGraph::V1(federated_graph) = FederatedGraph::from_sdl(&schema).unwrap();
    let config = engine_v2::VersionedConfig::V1(federated_graph).into_latest();
    let gateway = Arc::new(Gateway::new(
        config.into(),
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
        .route("/graphql", post(engine_post))
        .route("/graphql2", post(engine_post2))
        .route("/graphql3", post(engine_post3))
        .with_state(gateway);

    let address: std::net::SocketAddr = format!("0.0.0.0:5000")
        .parse()
        .expect("we just defined it above, it _must work_");

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let listener = tokio::net::TcpListener::bind(&address).await.unwrap();
        println!("Ready!");
        axum::serve(listener, app).await.unwrap()
    })
}

// 316 050.4188 req/s
async fn engine_post2(
    State(_gateway): State<Arc<Gateway>>,
    _headers: HeaderMap,
    Json(_request): Json<engine::Request>,
) -> impl IntoResponse {
    let bytes = serde_json::to_vec(&serde_json::json!({"data":{"topProducts":[{"upc":"1","name":"Table","price":899,"inStock":true,"reviews":[{"id":"1","body":"Love it!","author":{"id":"1","name":"Ada Lovelace","username":"@ada"}},{"id":"4","body":"Prefer something else.","author":{"id":"2","name":"Alan Turing","username":"@complete"}}]},{"upc":"2","name":"Couch","price":1299,"inStock":false,"reviews":[{"id":"2","body":"Too expensive.","author":{"id":"1","name":"Ada Lovelace","username":"@ada"}}]},{"upc":"3","name":"Chair","price":54,"inStock":true,"reviews":[{"id":"3","body":"Could be better.","author":{"id":"2","name":"Alan Turing","username":"@complete"}}]}]}}
    )).unwrap();
    (http::StatusCode::OK, http::HeaderMap::new(), bytes).into_response()
}

// 327 006.4545 req/s
async fn engine_post3() -> impl IntoResponse {
    let bytes = CELL.get_or_init(|| {
        serde_json::to_vec(&serde_json::json!({"data":{"topProducts":[{"upc":"1","name":"Table","price":899,"inStock":true,"reviews":[{"id":"1","body":"Love it!","author":{"id":"1","name":"Ada Lovelace","username":"@ada"}},{"id":"4","body":"Prefer something else.","author":{"id":"2","name":"Alan Turing","username":"@complete"}}]},{"upc":"2","name":"Couch","price":1299,"inStock":false,"reviews":[{"id":"2","body":"Too expensive.","author":{"id":"1","name":"Ada Lovelace","username":"@ada"}}]},{"upc":"3","name":"Chair","price":54,"inStock":true,"reviews":[{"id":"3","body":"Could be better.","author":{"id":"2","name":"Alan Turing","username":"@complete"}}]}]}}
        )).unwrap().into()

    }).clone();

    (http::StatusCode::OK, bytes).into_response()
}

// like cli 19 777.3790 req/s
// without unbounded channel 22 578.9592 req/s
// unchecked_engine_execute 22 541.6448 req/s
async fn engine_post(
    State(gateway): State<Arc<Gateway>>,
    headers: HeaderMap,
    Json(request): Json<engine::Request>,
) -> impl IntoResponse {
    use runtime::context::RequestContext as _;
    use ulid as _;

    // let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    let ctx = RequestContext {
        ray_id: String::new(),
        headers,
        // wait_until_sender: sender,
    };

    let response = gateway.unchecked_engine_execute(&ctx, request).await;

    // let session = gateway.authorize(ctx.headers_as_map().into()).await;
    // let response = match session {
    //     Some(session) => session.execute(&ctx, request).await,
    //     None => gateway_v2::Response::unauthorized(),
    // };

    // tokio::spawn(wait(receiver));
    (response.status, response.headers, response.bytes).into_response()
}

#[derive(Clone)]
struct RequestContext {
    ray_id: String,
    headers: http::HeaderMap,
    // wait_until_sender: UnboundedSender<BoxFuture<'static, ()>>,
}

#[async_trait::async_trait]
impl runtime::context::RequestContext for RequestContext {
    fn ray_id(&self) -> &str {
        &self.ray_id
    }

    async fn wait_until(&self, fut: BoxFuture<'static, ()>) {
        unimplemented!()
        // self.wait_until_sender
        //     .send(fut)
        //     .expect("Channel is not closed before finishing all wait_until");
    }

    fn headers(&self) -> &http::HeaderMap {
        &self.headers
    }
}

async fn wait(mut receiver: UnboundedReceiver<BoxFuture<'static, ()>>) {
    // Wait simultaneously on everything immediately accessible
    join_all(std::iter::from_fn(|| receiver.try_recv().ok())).await;
    // Wait sequentially on the rest
    while let Some(fut) = receiver.recv().await {
        fut.await;
    }
}
