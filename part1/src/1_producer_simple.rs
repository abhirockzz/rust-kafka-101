use std::{thread, time::Duration};

use rdkafka::{
    producer::{BaseProducer, BaseRecord},
    ClientConfig,
};

fn main() {
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        //for auth
        .set("security.protocol", "SASL_SSL")
        .set("sasl.mechanisms", "PLAIN")
        .set("sasl.username", "<update>")
        .set("sasl.password", "<update>")
        .create()
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

        thread::sleep(Duration::from_secs(3));
    }
}
