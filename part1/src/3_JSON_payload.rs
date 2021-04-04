use std::{thread, time::Duration};

use bytes::Bytes;
use rdkafka::{
    message::ToBytes,
    producer::{BaseProducer, BaseRecord, Producer, ProducerContext, ThreadedProducer},
    ClientConfig, ClientContext, Message,
};

fn main() {
    let producer: ThreadedProducer<ProduceCallbackLogger> = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        //for auth
        .set("security.protocol", "SASL_SSL")
        .set("sasl.mechanisms", "PLAIN")
        .set("sasl.username", "<update>")
        .set("sasl.password", "<update>")
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

//will not compile
/*impl ToBytes for User {
    fn to_bytes(&self) -> &[u8] {
        let b = serde_json::to_vec_pretty(&self).expect("json serialization failed");
        b.as_slice()
    }
}*/

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
        //let msg = dr.unwrap();

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
