use servicelib::runtime::{
    datastruct::KeyValue,
    serde::{
        BoolSerde, BytesSerde, Float32Serde, Float64Serde, Int8Serde, Int16ArraySerde, Int16Serde,
        Int32ArraySerde, Int32Serde, Int64ArraySerde, Int64Serde, RuneSerde, Serde,
        StringArraySerde, StringSerde, UInt16Serde, UInt32Serde, UInt64Serde,
        make_stream_key_value_serde,
    },
};
use std::sync::Arc;

fn emit<T>(name: &str, serde: &dyn Serde<T>, value: &T) {
    let data = serde
        .serialize(value)
        .unwrap_or_else(|error| panic!("{name}: {error}"));
    print!("{name}=");
    for byte in data {
        print!("{byte:02x}");
    }
    println!();
}

fn main() {
    emit("bool_false", &BoolSerde, &false);
    emit("bool_true", &BoolSerde, &true);
    emit("int8_negative", &Int8Serde, &-1_i8);
    emit("int16_min", &Int16Serde, &i16::MIN);
    emit("int16_negative", &Int16Serde, &-1_i16);
    emit("int16_zero", &Int16Serde, &0_i16);
    emit("int16_max", &Int16Serde, &i16::MAX);
    emit("int32_negative", &Int32Serde, &-1_i32);
    emit("int32_zero", &Int32Serde, &0_i32);
    emit("int64_negative", &Int64Serde, &-1_i64);
    emit("int64_zero", &Int64Serde, &0_i64);
    emit("uint16", &UInt16Serde, &0x0102_u16);
    emit("uint32", &UInt32Serde, &0x0102_0304_u32);
    emit("uint64", &UInt64Serde, &0x0102_0304_0506_0708_u64);
    emit("rune", &RuneSerde, &'Ж');
    emit("float32", &Float32Serde, &f32::from_bits(0x7fc0_1234));
    emit(
        "float64",
        &Float64Serde,
        &f64::from_bits(0x7ff8_0000_0000_1234),
    );
    emit("string", &StringSerde::default(), &"A\0B".to_owned());
    emit("bytes", &BytesSerde::default(), &vec![0x00, 0x7f, 0xff]);
    emit(
        "int16_array",
        &Int16ArraySerde::default(),
        &vec![i16::MIN, -1, 0, i16::MAX],
    );
    emit("int32_array", &Int32ArraySerde::default(), &vec![-1, 0, 1]);
    emit("int64_array", &Int64ArraySerde::default(), &vec![-1, 0, 1]);
    emit(
        "string_array",
        &StringArraySerde::default(),
        &vec!["one".to_owned(), String::new(), "three".to_owned()],
    );
    let key_value_serde =
        make_stream_key_value_serde(Arc::new(Int32Serde), Arc::new(StringSerde::default()));
    emit(
        "key_value",
        key_value_serde.as_ref(),
        &KeyValue {
            key: -7,
            value: "seven".to_owned(),
        },
    );
}
