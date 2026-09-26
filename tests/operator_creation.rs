use std::sync::Arc;

use servicelib::{
    operators::{delay, filter, flatmap, flatmapiterable, join, keyby, map, merge, process},
    runtime::{
        config::{
            DelayStreamConfig, FilterStreamConfig, FlatMapIterableStreamConfig,
            FlatMapStreamConfig, JoinStreamConfig, JoinType, KeyByStreamConfig, MapStreamConfig,
            MergeStreamConfig, ProcessStreamConfig, StreamConfig,
        },
        datastruct::KeyValue,
        environment::RuntimeEnvironment,
        serde::{JsonSerde, make_stream_key_value_serde, make_stream_serde},
        stream::Stream,
    },
};

#[tokio::test]
async fn preserving_operators_inherit_the_actual_source_serde() {
    let environment = RuntimeEnvironment::default();
    // Deliberately different from the environment's standard String codec.
    let source = Stream::<String>::derived(
        &StreamConfig::new(1, "source"),
        environment.clone(),
        make_stream_serde(Arc::new(JsonSerde::<String>::new())),
    );
    assert!(!Arc::ptr_eq(
        &source.get_serde(),
        &environment.make_serde::<String>(),
    ));
    let filtered = filter::create(
        &FilterStreamConfig::from(StreamConfig::new(2, "filter")),
        &source,
    );
    let delayed = delay::create(
        &DelayStreamConfig::from(StreamConfig::new(3, "delay")),
        &filtered,
    );
    let merged = merge::create(
        &MergeStreamConfig::from(StreamConfig::new(4, "merge")),
        &delayed,
    );
    for stream in [&filtered, &delayed, &merged] {
        assert!(Arc::ptr_eq(&source.get_serde(), &stream.get_serde()));
    }
}

#[tokio::test]
async fn transforming_operators_resolve_the_output_type_serde() {
    let environment = RuntimeEnvironment::default();
    let streams: [Stream<String>; 5] = [
        map::create(
            &MapStreamConfig::from(StreamConfig::new(1, "map")),
            environment.clone(),
        ),
        flatmap::create(
            &FlatMapStreamConfig::from(StreamConfig::new(2, "flatmap")),
            environment.clone(),
        ),
        flatmapiterable::create(
            &FlatMapIterableStreamConfig::from(StreamConfig::new(3, "iterable")),
            environment.clone(),
        ),
        process::create(
            &ProcessStreamConfig::from(StreamConfig::new(4, "process")),
            environment.clone(),
        ),
        join::create(
            &JoinStreamConfig {
                stream: StreamConfig::new(5, "join"),
                join_type: JoinType::Inner,
                join_storage: servicelib::api::JoinStorageType::HashMap,
                ttl: std::time::Duration::ZERO,
                renew_ttl: false,
            },
            environment.clone(),
        ),
    ];
    let serde = environment.make_serde::<String>();
    for stream in streams {
        assert!(Arc::ptr_eq(&serde, &stream.get_serde()));
    }
}

#[tokio::test]
async fn keyby_uses_the_key_value_codec() {
    let environment = RuntimeEnvironment::default();
    let stream: Stream<KeyValue<String, u32>> = keyby::create(
        &KeyByStreamConfig::from(StreamConfig::new(1, "keyby")),
        environment.clone(),
    );
    let expected = make_stream_key_value_serde::<String, u32>(
        environment.make_serde(),
        environment.make_serde(),
    );
    let value = KeyValue {
        key: "key".to_owned(),
        value: 42,
    };
    assert_eq!(
        stream.get_serde().serialize(&value).unwrap(),
        expected.serialize(&value).unwrap()
    );
}
