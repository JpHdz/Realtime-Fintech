use axum::{Json, Router, extract::{Path, State, ws::{Message, WebSocket, WebSocketUpgrade}}, response::IntoResponse, routing::get };
use dotenvy::dotenv;
use redis::{AsyncCommands, aio::ConnectionManager};
use serde::Serialize;
use sqlx::{Pool, Postgres, postgres::PgPoolOptions};
use tokio::{sync::broadcast, time::{sleep,Duration}};
use tokio_stream::StreamExt;
use std::{env, sync::Arc};
use tower_http::cors::CorsLayer;

// Share connections between routes
struct AppState{
    redis_client: ConnectionManager,
    tx: broadcast::Sender<String>,
    db_pool: Pool<Postgres>
}

// Parse to Json
#[derive(Serialize)]
 struct PriceResponse {
    symbol: String,
    price: f64,
    sma_20: f64,
    recommendation: String
 }

 #[derive(Serialize, sqlx::FromRow)]
 struct HistoryRecord {
    time: chrono::DateTime<chrono::Utc>,
    price: f64
 }

#[tokio::main]
async fn main() {

    dotenv().ok();

    let timescaledb_url = env::var("DATABASE_URL").expect("DATABASE URL not found");

    println!("Connecting to timescaledb: {}", timescaledb_url);

    let db_pool = PgPoolOptions::new().max_connections(5).connect(&timescaledb_url).await.expect("Failed to connect to TimescaleDB");


    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());

    let client = redis::Client::open(redis_url).expect("Invalid redis client");


    let connection_manager = loop {
        match client.get_connection_manager().await {
            Ok(conn) => {
                println!("Connected to Redis Manager!");
                break conn;
            }
            Err(e) => {
                eprintln!("Redis not ready yet: {}. Retrying in 2s...", e);
                sleep(Duration::from_secs(2)).await;
            }
        }
    };

    let (tx,_rx) = broadcast::channel(100);

    let state = Arc::new(AppState {redis_client: connection_manager, tx:tx.clone(), db_pool});
   
   let mut pubsub_conn = loop {
        match client.get_async_connection().await {
            Ok(conn) => {
                println!("Connected to Redis PubSub!");
                break conn.into_pubsub();
            }
            Err(e) => {
                eprintln!("Redis PubSub not ready: {}. Retrying in 2s...", e);
                sleep(Duration::from_secs(2)).await;
            }
        }
    };

    pubsub_conn.subscribe("updates").await.unwrap();

    tokio::spawn(async move {
        let mut pubsub_stream = pubsub_conn.on_message();
        println!("Listening Redis updates...");

        while let Some(message) = pubsub_stream.next().await {
                let payload = message.get_payload().unwrap();
                let _ = tx.send(payload);
        };
    });
  
    let app = Router::new().route("/api/v1/history/:symbol", get(history_btc)).route("/api/v1/btc", get(get_btc_data)).route("/ws", get(ws_handler)).layer(CorsLayer::permissive()).with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();  
    println!("Api gateway running on localhost:3000");
    axum::serve(listener,app).await.unwrap();  
}


async fn history_btc(Path(symbol): Path<String>,State(state): State<Arc<AppState>>) -> Json<Vec<HistoryRecord>> {
    let records = sqlx::query_as!(HistoryRecord, "SELECT time, price FROM trades WHERE symbol = $1 ORDER BY time DESC LIMIT 100", symbol).fetch_all(&state.db_pool).await.unwrap_or_else(|e|{
         eprintln!("Error reading from db: {}", e);
         vec![]
    });
    Json(records)
}
// Symbol ex. .../history/BTCUSDT <- Symbol
async fn get_btc_data(State(state): State<Arc<AppState>>) -> Json<PriceResponse>{
    
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

    // Use Json(serde_json::json!(response)) when returning a generic json to the client
    // Parse to json
    Json(response)
}

async fn ws_handler(ws:WebSocketUpgrade, State(state): State<Arc<AppState>>) -> impl IntoResponse { 
    ws.on_upgrade(|socket| handle_socket(socket,state))
}

async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>){
    let mut rx = state.tx.subscribe();
    while let Ok(msg) = rx.recv().await {
        if socket.send(Message::Text(msg)).await.is_err() {
            break;
        }
    }
}