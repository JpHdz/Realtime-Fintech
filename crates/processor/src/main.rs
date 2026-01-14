use std::collections::VecDeque;
use std::env;
use std::time::Duration;

use chrono::{DateTime, Utc};
use common::Trade;
use prost::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer,StreamConsumer};
use rdkafka::message::Message as KafkaMessage;
use redis::aio::MultiplexedConnection;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres, QueryBuilder};
use dotenvy::dotenv;
use tokio::time::interval;
use serde_json::json;

use common::observability;

use tracing::{Instrument, error, info, info_span, instrument};


struct TradeDb {
    symbol: String,
    price: f64,
    time: DateTime<Utc>
}


const WINDOW_SIZE: usize = 10;


fn create_consumer() -> StreamConsumer {
    let kafka_broker = env::var("KAFKA_BROKER").unwrap_or_else(|_| "localhost:9094".to_string());
    ClientConfig::new().set("group.id", "crypto-proccesor-group").set("bootstrap.servers", &kafka_broker).set("enable.auto.commit", "true").set("auto.offset.reset", "earliest").create().expect("Fail creating consumer")
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    observability::init_tracing("crypto-processor");
    dotenv().ok();

    let timescaledb_url = env::var("DATABASE_URL").expect("DATABASE URL not found");
    
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let redis_client = redis::Client::open(redis_url).expect("Invalid redis client");

    let db_pool  = PgPoolOptions::new().max_connections(5).connect(&timescaledb_url).await?;
    info!("Connecting to timescaledb");

    let mut redis_connection = redis_client.get_multiplexed_async_connection().await?;
    info!("Connecting to redis");    

    let consumer = create_consumer();
    loop {
    match consumer.subscribe(&["trades"]) {
            Ok(_) => {
                info!("Subscribed to trades topic");
                break;
            },
            Err(e) => {
                error!("Failed to subscribe to 'trades': {}. Retrying in 5 seconds...", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }   
    info!("Started processing data...");
    
    let mut price_window: VecDeque<f64> = VecDeque::with_capacity(WINDOW_SIZE);
    let max_batch_size = 100;
    let batch_timeout = Duration::from_millis(500);
    let mut buffer:Vec<TradeDb> = Vec::with_capacity(max_batch_size);
    let mut timer = interval(batch_timeout);

    loop {
        tokio::select! {
            msg_result = consumer.recv() => {
                match msg_result {
                        Ok(msg) =>{
                            if let Some(payload) = msg.payload() {
                                match Trade::decode(payload) {
                                    Ok(trade) => {
                                        process_trade(&mut price_window, trade, &mut buffer, max_batch_size, &db_pool, &mut timer, &mut redis_connection).await
                                    } Err(error) => error!("Error while decoding payload: {}", error)
                                }
                        };
                    }Err(error) => error!("Kafka error: {}", error)
                }
            }
            _ = timer.tick() => {
                 flush_buffer(&db_pool, &mut buffer).await;
                 timer.reset();
            }
        }
    }
    Ok(())
}


#[instrument(skip(pool,buffer), fields(batch_size = buffer.len()))]
async fn flush_buffer(pool:&Pool<Postgres>,buffer: &mut Vec<TradeDb>) {
    if buffer.is_empty() { return };

    let db_span = info_span!("sql_bulk_insert");

    let mut query_builder = QueryBuilder::new( "INSERT INTO TRADES (symbol, price, time) ");

    query_builder.push_values(buffer.iter(), |mut b, trade| {
        b.push_bind(&trade.symbol).push_bind(trade.price).push_bind(trade.time);
    });

    let query = query_builder.build();

    if let Err(error) = query.execute(pool).instrument(db_span).await {
        error!("There has been an error inserting batch into DB: {}", error);
    } else {
        info!("Batch inserted into DB");
    }
    buffer.clear();
}


#[instrument(skip(price_window, buffer, redis_connection,db_pool,timer),fields(symbol = %trade.symbol, price = trade.price))]
async fn process_trade(price_window: &mut VecDeque<f64>, 
    trade: Trade, 
    buffer: &mut Vec<TradeDb>, 
    max_batch_size: usize,
    db_pool: &Pool<Postgres>, 
    timer: &mut tokio::time::Interval, 
    redis_connection: &mut MultiplexedConnection) 
    {
    
    price_window.push_back(trade.price);

    if price_window.len() > 10{
        price_window.pop_front();
    }
    let sum: f64 = price_window.iter().sum();
    let avg: f64 = sum / price_window.len() as f64;

    let json_msg = json!({
        "symbol": trade.symbol,
        "price": trade.price,
        "sma": avg,
        "timestamp": trade.timestamp
    }).to_string(); 

    let redis_span = info_span!("redis_publish");   
    let _ : () = redis::pipe()
    .set("btc_price", trade.price)
    .set("btc_sma", avg)
    .publish("updates", json_msg)
    .query_async( redis_connection)
    .instrument(redis_span)
    .await.unwrap_or_else(|error| error!("Error sending data to redis: {}", error));                                   

    info!("Price {:.2} | SMA:({}):${:.2} | Buffer: {}/{}",trade.price, WINDOW_SIZE , avg, price_window.len(), WINDOW_SIZE);

    let time_stamp_converted = DateTime::from_timestamp_millis(trade.timestamp as i64);

    if let Some(timestamp) = time_stamp_converted {
        buffer.push(TradeDb { symbol: trade.symbol, price: trade.price, time: timestamp });   
            if buffer.len() >= max_batch_size {
                flush_buffer(&db_pool, buffer).await;
                    timer.reset();
                } 
        }
}
