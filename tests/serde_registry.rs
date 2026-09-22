use servicelib::runtime::{
    config::StreamConfig,
    environment::{RuntimeEnvironment, RuntimeError, RuntimeResult},
    serde::{JsonSerde, Serde, Serializer, StringSerde},
    stream::Stream,
};
use std::{any::TypeId, sync::Arc};

struct NoSerde;

fn json_strings(value_type: TypeId, _: &RuntimeEnvironment) -> RuntimeResult<Option<Serializer>> {
    Ok((value_type == TypeId::of::<String>())
        .then(|| Serializer::new::<String>(Arc::new(JsonSerde::<String>::new()))))
}

fn wrong_type(_: TypeId, _: &RuntimeEnvironment) -> RuntimeResult<Option<Serializer>> {
    Ok(Some(Serializer::new::<String>(Arc::new(
        StringSerde::default(),
    ))))
}

fn failed_factory(_: TypeId, _: &RuntimeEnvironment) -> RuntimeResult<Option<Serializer>> {
    Err(RuntimeError::InvalidConfiguration(
        "custom serde failed".to_owned(),
    ))
}

#[tokio::test]
async fn missing_serde_does_not_require_serde_derive() {
    let environment = RuntimeEnvironment::default();
    let stream = Stream::<NoSerde>::new(&StreamConfig::new(1, "Plain"), environment.clone());
    let serde = stream.get_serde();
    assert!(serde.is_stub());
    assert!(serde.serialize(&NoSerde).is_err());
    assert!(serde.deserialize(&[]).is_err());
    assert!(Arc::ptr_eq(
        &serde,
        &environment.get_serde::<NoSerde>().unwrap()
    ));
}

#[tokio::test]
async fn standard_serde_matches_existing_binary_codec() {
    let environment = RuntimeEnvironment::default();
    let serde = environment.get_serde::<String>().unwrap();
    let value = "hello".to_owned();
    assert_eq!(
        serde.serialize(&value).unwrap(),
        StringSerde::default().serialize(&value).unwrap()
    );
    assert!(!serde.is_stub());
    assert!(Arc::ptr_eq(
        &serde,
        &environment.get_serde::<String>().unwrap()
    ));
}

#[tokio::test]
async fn custom_factory_overrides_standard_and_is_service_scoped() {
    let environment = RuntimeEnvironment::default();
    let custom = environment.for_service(1);
    let standard = environment.for_service(2);
    custom.set_serde_provider(json_strings).unwrap();
    let value = "hello".to_owned();
    assert_eq!(
        custom
            .get_serde::<String>()
            .unwrap()
            .serialize(&value)
            .unwrap(),
        b"\"hello\""
    );
    assert_eq!(
        standard
            .get_serde::<String>()
            .unwrap()
            .serialize(&value)
            .unwrap(),
        StringSerde::default().serialize(&value).unwrap()
    );
    assert!(custom.set_serde_provider(json_strings).is_err());
}

#[tokio::test]
async fn provider_type_mismatch_is_not_an_unchecked_cast() {
    let environment = RuntimeEnvironment::default();
    environment.set_serde_provider(wrong_type).unwrap();
    assert!(environment.get_serde::<i32>().is_err());
    assert!(environment.make_serde::<i32>().is_stub());
}

#[tokio::test]
async fn failed_provider_matches_go_stub_fallback() {
    let environment = RuntimeEnvironment::default();
    environment.set_serde_provider(failed_factory).unwrap();
    assert!(environment.get_serde::<String>().is_err());
    assert!(environment.make_serde::<String>().is_stub());
}

#[tokio::test]
async fn streams_cache_failed_provider_stub_per_service_and_type() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    static CALLS: AtomicUsize = AtomicUsize::new(0);

    fn counted_failure(
        value_type: TypeId,
        environment: &RuntimeEnvironment,
    ) -> RuntimeResult<Option<Serializer>> {
        CALLS.fetch_add(1, Ordering::SeqCst);
        failed_factory(value_type, environment)
    }

    let environment = RuntimeEnvironment::default();
    let first_service = environment.for_service(1);
    let second_service = environment.for_service(2);
    first_service.set_serde_provider(counted_failure).unwrap();
    second_service.set_serde_provider(counted_failure).unwrap();

    let first = Stream::<String>::new(&StreamConfig::new(1, "First"), first_service.clone());
    let second = Stream::<String>::new(&StreamConfig::new(2, "Second"), first_service.clone());
    let serde = first.get_serde();
    assert!(serde.is_stub());
    assert!(serde.serialize(&"value".to_owned()).is_err());
    assert!(Arc::ptr_eq(&serde, &second.get_serde()));
    assert!(Arc::ptr_eq(
        &serde,
        &first_service.get_serde::<String>().unwrap()
    ));
    assert_eq!(CALLS.load(Ordering::SeqCst), 1);

    let other_type = first_service.make_serde::<i32>();
    assert!(other_type.is_stub());
    assert!(Arc::ptr_eq(&other_type, &first_service.make_serde::<i32>()));
    assert_eq!(CALLS.load(Ordering::SeqCst), 2);

    let other_service = second_service.make_serde::<String>();
    assert!(other_service.is_stub());
    assert!(!Arc::ptr_eq(&serde, &other_service));
    assert!(Arc::ptr_eq(
        &other_service,
        &second_service.make_serde::<String>()
    ));
    assert_eq!(CALLS.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn concurrent_resolutions_return_the_same_cached_instance() {
    let environment = RuntimeEnvironment::default();
    environment.set_serde_provider(json_strings).unwrap();
    let mut tasks = Vec::new();
    for _ in 0..32 {
        let environment = environment.clone();
        tasks.push(tokio::spawn(async move {
            environment.get_serde::<String>().unwrap()
        }));
    }
    let expected = environment.get_serde::<String>().unwrap();
    for task in tasks {
        assert!(Arc::ptr_eq(&expected, &task.await.unwrap()));
    }
}
