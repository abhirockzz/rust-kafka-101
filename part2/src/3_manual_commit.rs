use std::{thread, time::Duration};

use rand::Rng;
use rdkafka::{
    consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance},
    message::ToBytes,
    producer::{BaseProducer, BaseRecord, Producer, ProducerContext, ThreadedProducer},
    ClientConfig, ClientContext, Message, Offset, TopicPartitionList,
};

fn main() {
    let consumer: BaseConsumer<ConsumerCallbackLogger> = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        //for auth
        /*.set("security.protocol", "SASL_SSL")
        .set("sasl.mechanisms", "PLAIN")
        .set("sasl.username", "<update>")
        .set("sasl.password", "<update>")*/
        .set("group.id", "my_consumer_group")
        .set("enable.auto.commit", "false")
        .create_with_context(ConsumerCallbackLogger {})
        .expect("invalid consumer config");

    consumer
        .subscribe(&["rust"])
        .expect("topic subscribe failed");

    thread::spawn(move || 'consumer_thread: loop {
        for msg_result in consumer.iter() {
            let msg = msg_result.unwrap();
            let key: &str = msg.key_view().unwrap().unwrap();
            let value = msg.payload().unwrap();
            let user: User =
                serde_json::from_slice(value).expect("failed to deserialize JSON to User");

            println!(
                "received key {} with value {:?} in offset {:?} from partition {}",
                key,
                user,
                msg.offset(),
                msg.partition()
            );

            let processed = process(user);
            match processed {
                Ok(_) => {
                    consumer.commit_message(&msg, CommitMode::Sync);
                }
                Err(_) => {
                    println!("loop encountered processing error. closing consumer...");
                    break 'consumer_thread;
                }
            }
        }
    });

    let producer: ThreadedProducer<ProduceCallbackLogger> = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        //for auth
        /*.set("security.protocol", "SASL_SSL")
        .set("sasl.mechanisms", "PLAIN")
        .set("sasl.username", "<update>")
        .set("sasl.password", "<update>")*/
        .create_with_context(ProduceCallbackLogger {})
        .expect("invalid producer config");

    for i in 1..100 {
        let user = User {
            id: i,
            email: format!("user-{}@foobar.com", i),
        };

        let user_json = serde_json::to_string_pretty(&user).expect("json serialization failed");

        println!("sending message");

        producer
            .send(
                BaseRecord::to("rust")
                    .key(&format!("user-{}", i))
                    .payload(&user_json),
            )
            .expect("failed to send message");

        thread::sleep(Duration::from_secs(3));
    }
}

fn process(u: User) -> Result<(), ()> {
    let mut rnd = rand::thread_rng();
    let ok = rnd.gen_bool(1.0 / 2.0); //50% probability of returning true
    match ok {
        true => {
            println!("SUCCESSFULLY processed User info {:?}", u);
            Ok(())
        }
        false => {
            println!("FAILED to process User info {:?}", u);
            Err(())
        }
    }
}

use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize, Debug)]
struct User {
    id: i32,
    email: String,
}

struct ConsumerCallbackLogger;

impl ClientContext for ConsumerCallbackLogger {}

impl ConsumerContext for ConsumerCallbackLogger {
    fn pre_rebalance<'a>(&self, _rebalance: &rdkafka::consumer::Rebalance<'a>) {}

    fn post_rebalance<'a>(&self, rebalance: &rdkafka::consumer::Rebalance<'a>) {
        println!("post_rebalance callback");

        match rebalance {
            Rebalance::Assign(tpl) => {
                for e in tpl.elements() {
                    println!("rebalanced partition {}", e.partition())
                }
            }
            Rebalance::Revoke => {
                println!("ALL partitions have been REVOKED")
            }
            Rebalance::Error(err_info) => {
                println!("Post Rebalance error {}", err_info)
            }
        }
    }

    fn commit_callback(
        &self,
        result: rdkafka::error::KafkaResult<()>,
        offsets: &rdkafka::TopicPartitionList,
    ) {
        match result {
            Ok(_) => {
                for e in offsets.elements() {
                    match e.offset() {
                        //skip Invalid offset
                        Offset::Invalid => {}
                        _ => {
                            println!(
                                "committed offset {:?} in partition {}",
                                e.offset(),
                                e.partition()
                            )
                        }
                    }
                }
            }
            Err(err) => {
                println!("error committing offset - {}", err)
            }
        }
    }
}

struct ProduceCallbackLogger;

impl ClientContext for ProduceCallbackLogger {}

impl ProducerContext for ProduceCallbackLogger {
    type DeliveryOpaque = ();

    fn delivery(
        &self,
        delivery_result: &rdkafka::producer::DeliveryResult<'_>,
        _delivery_opaque: Self::DeliveryOpaque,
    ) {
        let dr = delivery_result.as_ref();
        match dr {
            Ok(msg) => {
                let key: &str = msg.key_view().unwrap().unwrap();
                println!(
                    "produced message with key {} in offset {} of partition {}",
                    key,
                    msg.offset(),
                    msg.partition()
                )
            }
            Err(producer_err) => {
                let key: &str = producer_err.1.key_view().unwrap().unwrap();

                println!(
                    "failed to produce message with key {} - {}",
                    key, producer_err.0,
                )
            }
        }
    }
}
