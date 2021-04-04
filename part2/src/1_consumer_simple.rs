use std::{thread, time::Duration};

use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    message::ToBytes,
    producer::{BaseProducer, BaseRecord, Producer, ProducerContext, ThreadedProducer},
    ClientConfig, ClientContext, Message,
};

fn main() {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        //for auth
        /*.set("security.protocol", "SASL_SSL")
        .set("sasl.mechanisms", "PLAIN")
        .set("sasl.username", "<update>")
        .set("sasl.password", "<update>")*/
        .set("group.id", "my_consumer_group")
        .create()
        .expect("invalid consumer config");

    consumer
        .subscribe(&["rust"])
        .expect("topic subscribe failed");

    thread::spawn(move || loop {
        for msg_result in consumer.iter() {
            let msg = msg_result.unwrap();
            let key: &str = msg.key_view().unwrap().unwrap();
            let value = msg.payload().unwrap();
            let user: User = serde_json::from_slice(value).expect("failed to deser JSON to User");
            println!(
                "received key {} with value {:?} in offset {:?} from partition {}",
                key,
                user,
                msg.offset(),
                msg.partition()
            )
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
        println!("sending message");

        let user = User {
            id: i,
            email: format!("user-{}@foobar.com", i),
        };

        let user_json = serde_json::to_string_pretty(&user).expect("json serialization failed");
        //let user_json = serde_json::to_vec_pretty(&user).expect("json serialization failed");

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

use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize, Debug)]
struct User {
    id: i32,
    email: String,
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
