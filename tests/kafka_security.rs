use rdkafka::{ClientConfig, producer::FutureProducer};

#[test]
fn kafka_scram_sha_512_provider_is_compiled_in() {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", "127.0.0.1:1")
        .set("security.protocol", "SASL_PLAINTEXT")
        .set("sasl.mechanism", "SCRAM-SHA-512")
        .set("sasl.username", "conformance")
        .set("sasl.password", "conformance")
        .set("message.timeout.ms", "1");

    let producer: Result<FutureProducer, _> = config.create();
    if let Err(error) = producer {
        panic!("Rust Kafka runtime must include a SCRAM-SHA-512 provider: {error}");
    }
}
