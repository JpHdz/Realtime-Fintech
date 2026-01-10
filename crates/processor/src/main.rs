use std::collections::VecDeque;

use common::Trade;
use prost::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer,StreamConsumer};
use rdkafka::message::Message as KafkaMessage;

const WINDOW_SIZE: usize = 20;


fn create_consumer() -> StreamConsumer {
    ClientConfig::new().set("group.id", "crypto-proccesor-group").set("bootstrap.servers", "localhost:9094").set("enable.auto.commit", "true").set("auto.offset.reset", "earliest").create().expect("Fail creating consumer")
}


#[tokio::main]
async fn main() {
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

                            println!("Price {:.2} | SMA:({}):${:.2} | Buffer: {}/{}",trade.price, WINDOW_SIZE , avg, price_window.len(), WINDOW_SIZE);

                        } Err(error) => eprintln!("Error while decoding payload: {}", error)
                    }
                }
            }Err(error) => eprintln!("Kafka error: {}", error)
        }
    }
}
