use std::env;
use std::io::Write;

use common::Trade;
use futures_util::StreamExt;
use serde::{self,Deserialize};
use tokio_tungstenite::connect_async;
use url::Url;
use prost::Message;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;


// Take json from binance and convert it to struct 
#[derive(Debug, Deserialize)]
struct BinanceTrade {
    s: String,
    p: String,
    q: String,
    #[serde(rename = "T")]
    timestamp: u64
}


fn create_producer() -> FutureProducer {
    let kafka_broker = env::var("KAFKA_BROKER").unwrap_or_else(|_| "localhost:9094".to_string());
    println!("Connecting to Kafka at: {}", kafka_broker);
    ClientConfig::new().set("bootstrap.servers", &kafka_broker).set("message.timeout.ms", "5000").create().expect("Failtd to initialize kafka producer")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    // Initialize kafka producer
    let kafka_producer = create_producer();
    let topic_name = "trades";

    // Connect to binance API   
    let binance_url = "wss://stream.binance.com:9443/ws/btcusdt@trade";
    let url = Url::parse(binance_url)?;

    println!("Connecting to binance");


    let (ws_stream, _ ) = connect_async(url).await?;

    println!("Conected to binance... Listening BTC/USTD trades");

    let (_, mut read) = ws_stream.split();


    while let Some(message) = read.next().await {
        match message {
            Ok(data) => {
                if let Ok(text) = data.to_text(){
                    if let Ok(incoming) = serde_json::from_str::<BinanceTrade>(text){
                        // Create Rust struct provided by proto
                        let trade = Trade {
                            symbol: incoming.s,
                            price: incoming.p.parse().unwrap_or(0.0),
                            quantity: incoming.q.parse().unwrap_or(0.0),
                            timestamp: incoming.timestamp
                        };

                        println!("Processing new trade: {:?}", trade);

                        // Convert trade to send only bytes into kafka
                        let mut payload = Vec::new();
                        trade.encode(&mut payload)?;

                        let record = FutureRecord::to(topic_name).payload(&payload).key("BTCUSDT");

                        match kafka_producer.send(record, Timeout::Never).await {
                            Ok(delivery) => {
                                println!("Delivery: {:?}", delivery);
                            } Err(error) => eprintln!("Error sending record to kafka: {:?}", error)
                        }

                        
                    }

                }
            }Err(error) => eprintln!("Error reading message in socket: {}", error)
        }
    }

    Ok(())
}