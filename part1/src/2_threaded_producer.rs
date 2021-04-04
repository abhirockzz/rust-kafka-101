use std::{thread, time::Duration};

use rdkafka::{
    producer::{BaseProducer, BaseRecord, Producer, ProducerContext, ThreadedProducer},
    ClientConfig, ClientContext, Message,
};

fn main() {
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

        producer
            .send(
                BaseRecord::to("rust")
                    .key(&format!("key-{}", i))
                    .payload(&format!("value-{}", i)),
            )
            .expect("failed to send message");

        //producer.flush(Duration::from_secs(3));
        //println!("flushed message");
        thread::sleep(Duration::from_secs(3));
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
