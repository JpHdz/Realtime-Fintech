use axum::{extract::State,routing::get,Json, Router};
use redis::{AsyncCommands, aio::ConnectionManager};
use serde::Serialize;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
// Share connections between routes
struct AppState{
    redis_client: ConnectionManager,
}

// Parse to Json
#[derive(Serialize)]
 struct PriceResponse {
    symbol: String,
    price: f64,
    sma_20: f64,
    recommendation: String
 }


#[tokio::main]
async fn main() {

    let client = redis::Client::open("redis://127.0.0.1:6379").expect("Invalid redis client");

    let state = Arc::new(AppState {redis_client: client.get_connection_manager().await.unwrap()});
   
    let app = Router::new().route("/api/v1/btc", get(get_btc_data)).layer(CorsLayer::permissive()).with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();  
    println!("Api gateway running on localhost:3000");
    axum::serve(listener,app).await.unwrap();  
}

async fn get_btc_data(State(state): State<Arc<AppState>>) -> Json<serde_json::Value>{
    
    let mut connection = state.redis_client.clone();
    let price: f64 = connection.get("btc_price").await.unwrap_or(0.0);
    let sma : f64 = connection.get("btc_sma").await.unwrap_or(0.0);

    let signal = if price > sma {
        "PURCHASE"
    } else {
        "SELL"
    };

    let response = PriceResponse {
        symbol: "BTCUSDT".to_string(),
        price,
        sma_20: sma,
        recommendation: signal.to_string()
    };

    // Parse to json
    Json(serde_json::json!(response))
}