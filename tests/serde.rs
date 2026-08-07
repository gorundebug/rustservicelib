use serde::{Deserialize, Serialize};
use servicelib::runtime::serde::{
    ArraySerde, Float32Serde, Int16ArraySerde, Int16Serde, Int32Serde, JsonSerde, MapSerde, Serde,
    SerdeLimits, StringArraySerde, StringSerde, make_stream_key_value_serde, make_stream_serde,
};
use std::{collections::HashMap, sync::Arc};

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Value {
    id: u64,
    name: String,
}

#[test]
fn json_round_trip() {
    let serde = JsonSerde::<Value>::new();
    let value = Value {
        id: 42,
        name: "test".to_owned(),
    };
    let encoded = serde.serialize(&value).unwrap();
    assert_eq!(serde.deserialize(&encoded).unwrap(), value);
}

#[test]
fn primitive_wire_format_matches_go() {
    assert_eq!(Int16Serde.serialize(&i16::MIN).unwrap(), [0x00, 0x00]);
    assert_eq!(Int16Serde.serialize(&-1).unwrap(), [0x7f, 0xff]);
    assert_eq!(Int16Serde.serialize(&0).unwrap(), [0x80, 0x00]);
    assert_eq!(Int32Serde.serialize(&-1).unwrap(), [0x7f, 0xff, 0xff, 0xff]);
    assert_eq!(
        Float32Serde.serialize(&1.0).unwrap(),
        [0x3f, 0x80, 0x00, 0x00]
    );
}

#[test]
fn strings_fixed_arrays_and_framed_arrays_round_trip() {
    let string_serde = StringSerde::default();
    let value = String::from("A\0B");
    assert_eq!(
        string_serde.serialize(&value).unwrap(),
        [0, 0, 0, 0, 0, 0, 0, 3, b'A', 0, b'B']
    );
    assert_eq!(
        string_serde
            .deserialize(&string_serde.serialize(&value).unwrap())
            .unwrap(),
        value
    );

    let fixed_serde = Int16ArraySerde::default();
    let fixed = vec![i16::MIN, -1, 0, i16::MAX];
    assert_eq!(
        fixed_serde.serialize(&fixed).unwrap(),
        [
            0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0x7f, 0xff, 0x80, 0, 0xff, 0xff
        ]
    );
    assert_eq!(
        fixed_serde
            .deserialize(&fixed_serde.serialize(&fixed).unwrap())
            .unwrap(),
        fixed
    );

    let framed = ArraySerde::new(Arc::new(StringSerde::default()));
    let values = vec!["one".to_owned(), String::new(), "three".to_owned()];
    assert_eq!(
        framed
            .deserialize(&framed.serialize(&values).unwrap())
            .unwrap(),
        values
    );
}

#[test]
fn map_round_trip_and_limits() {
    let map_serde = MapSerde::new(
        Arc::new(StringArraySerde::default()),
        Arc::new(Int16ArraySerde::default()),
    );
    let values = HashMap::from([("alpha".to_owned(), 1_i16), ("beta".to_owned(), -2_i16)]);
    assert_eq!(
        map_serde
            .deserialize(&map_serde.serialize(&values).unwrap())
            .unwrap(),
        values
    );

    let limited = StringSerde::new(SerdeLimits {
        max_string_bytes: 3,
        ..SerdeLimits::default()
    });
    assert!(limited.serialize(&"four".to_owned()).is_err());
    let truncated = [0, 0, 0, 0, 0, 0, 0, 5, b'a', b'b'];
    let error = StringSerde::default().deserialize(&truncated).unwrap_err();
    assert_eq!(error.offset(), 8);
}

#[test]
fn stream_and_key_value_wrappers_match_transport_semantics() {
    let stream_serde = make_stream_serde(Arc::new(StringSerde::default()));
    assert!(!stream_serde.is_key_value());
    let value = "payload".to_owned();
    assert_eq!(
        stream_serde
            .deserialize(&stream_serde.serialize(&value).unwrap())
            .unwrap(),
        value
    );

    let key_value_serde =
        make_stream_key_value_serde(Arc::new(Int32Serde), Arc::new(StringSerde::default()));
    let value = servicelib::runtime::datastruct::KeyValue {
        key: -7,
        value: "seven".to_owned(),
    };
    assert!(key_value_serde.is_key_value());
    let key = key_value_serde.serialize_key(&value).unwrap().unwrap();
    let encoded_value = key_value_serde.serialize_value(&value).unwrap();
    let decoded = key_value_serde
        .deserialize_key_value(Some(&key), &encoded_value)
        .unwrap();
    assert_eq!(decoded.key, -7);
    assert_eq!(decoded.value, "seven");
}
