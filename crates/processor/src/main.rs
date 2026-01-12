use std::collections::VecDeque;
use std::env;

use chrono::DateTime;
use common::Trade;
use prost::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer,StreamConsumer};
use rdkafka::message::Message as KafkaMessage;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Pool, Postgres};
use dotenvy::dotenv;


const WINDOW_SIZE: usize = 20;


fn create_consumer() -> StreamConsumer {
    ClientConfig::new().set("group.id", "crypto-proccesor-group").set("bootstrap.servers", "localhost:9094").set("enable.auto.commit", "true").set("auto.offset.reset", "earliest").create().expect("Fail creating consumer")
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    dotenv().ok();

    let timescaledb_url = env::var("DATABASE_URL").expect("DATABASE URL not found");

    let redis_client = redis::Client::open("redis://127.0.0.1:6379")?;
    
    let db_pool  = PgPoolOptions::new().max_connections(5).connect(&timescaledb_url).await?;

    println!("Connecting to timescaledb");

    let mut redis_connection = redis_client.get_async_connection().await?;

    println!("Connecting to redis");    

    let consumer = create_consumer();
    consumer.subscribe(&["trades"]).expect("Failed subscribing to topic");
    println!("Started processing data...");
    
    let mut price_window: VecDeque<f64> = VecDeque::with_capacity(WINDOW_SIZE);

    loop {
        match consumer.recv().await {
            Ok(msg) =>{
                if let Some(payload) = msg.payload() {
                    match Trade::decode(payload) {
                        Ok(trade) => {
                            price_window.push_back(trade.price);

                            if price_window.len() > 10{
                                price_window.pop_front();
                            }

                            let sum: f64 = price_window.iter().sum();
                            let avg: f64 = sum / 10.0;

                            let _ : () = redis::pipe().set("btc_price", trade.price).set("btc_sma", avg).query_async(&mut redis_connection).await.unwrap_or_else(|error| eprintln!("Error sending data to redis: {}", error));

                            println!("Price {:.2} | SMA:({}):${:.2} | Buffer: {}/{}",trade.price, WINDOW_SIZE , avg, price_window.len(), WINDOW_SIZE);

                            let time_stamp_converted = DateTime::from_timestamp_millis(trade.timestamp as i64);


                            if let Some(timestamp) = time_stamp_converted {
                                let insert_result = sqlx::query!(
                                    "INSERT INTO TRADES (symbol, price, time) VALUES ($1,$2,$3)", trade.symbol, trade.price, timestamp
                                ).execute(&db_pool).await;

                                if let Err(error) = insert_result {
                                    eprintln!("Failed to insert data into timescaledb: {}", error);
                                }
                            }

                        } Err(error) => eprintln!("Error while decoding payload: {}", error)
                    }
                }
            }Err(error) => eprintln!("Kafka error: {}", error)
        }
    }
    Ok(())
}
