use std::{collections::HashMap, hash::Hash, marker::PhantomData, sync::Arc};

use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::runtime::datastruct::KeyValue;

const SIZE_BYTES: usize = 8;

#[derive(Debug, Error)]
#[error("{message} at byte {offset}")]
pub struct SerdeError {
    message: String,
    offset: usize,
}

impl SerdeError {
    fn new(message: impl Into<String>, offset: usize) -> Self {
        Self {
            message: message.into(),
            offset,
        }
    }

    pub fn offset(&self) -> usize {
        self.offset
    }
}

#[derive(Clone, Copy, Debug)]
pub struct SerdeLimits {
    pub max_string_bytes: usize,
    pub max_bytes: usize,
    pub max_container_elements: usize,
    pub max_total_bytes: usize,
}

impl Default for SerdeLimits {
    fn default() -> Self {
        Self {
            max_string_bytes: usize::MAX,
            max_bytes: usize::MAX,
            max_container_elements: usize::MAX,
            max_total_bytes: usize::MAX,
        }
    }
}

pub trait Serde<T>: Send + Sync {
    fn serialize(&self, value: &T) -> Result<Vec<u8>, SerdeError>;
    fn deserialize(&self, value: &[u8]) -> Result<T, SerdeError>;

    fn serialize_to(&self, output: &mut Vec<u8>, value: &T) -> Result<(), SerdeError> {
        output.extend(self.serialize(value)?);
        Ok(())
    }

    fn is_stub(&self) -> bool {
        false
    }
}

pub trait StreamSerde<T>: Serde<T> {
    fn is_key_value(&self) -> bool {
        false
    }

    fn serialize_key(&self, _value: &T) -> Result<Option<Vec<u8>>, SerdeError> {
        Ok(None)
    }

    fn serialize_value(&self, value: &T) -> Result<Vec<u8>, SerdeError> {
        self.serialize(value)
    }

    fn deserialize_key_value(&self, _key: Option<&[u8]>, value: &[u8]) -> Result<T, SerdeError> {
        self.deserialize(value)
    }
}

struct ValueStreamSerde<T> {
    value_serde: Arc<dyn Serde<T>>,
}

impl<T> Serde<T> for ValueStreamSerde<T>
where
    T: Send + Sync + 'static,
{
    fn serialize(&self, value: &T) -> Result<Vec<u8>, SerdeError> {
        self.value_serde.serialize(value)
    }

    fn deserialize(&self, value: &[u8]) -> Result<T, SerdeError> {
        self.value_serde.deserialize(value)
    }

    fn is_stub(&self) -> bool {
        self.value_serde.is_stub()
    }
}

impl<T> StreamSerde<T> for ValueStreamSerde<T> where T: Send + Sync + 'static {}

pub fn make_stream_serde<T>(value_serde: Arc<dyn Serde<T>>) -> Arc<dyn StreamSerde<T>>
where
    T: Send + Sync + 'static,
{
    Arc::new(ValueStreamSerde { value_serde })
}

pub struct StreamKeyValueSerde<K, V> {
    key_serde: Arc<dyn Serde<K>>,
    value_serde: Arc<dyn Serde<V>>,
}

impl<K, V> StreamKeyValueSerde<K, V> {
    pub fn new(key_serde: Arc<dyn Serde<K>>, value_serde: Arc<dyn Serde<V>>) -> Self {
        Self {
            key_serde,
            value_serde,
        }
    }
}

impl<K, V> Serde<KeyValue<K, V>> for StreamKeyValueSerde<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    fn serialize(&self, value: &KeyValue<K, V>) -> Result<Vec<u8>, SerdeError> {
        let key = self.key_serde.serialize(&value.key)?;
        let encoded_value = self.value_serde.serialize(&value.value)?;
        let mut output = Vec::with_capacity(SIZE_BYTES * 2 + key.len() + encoded_value.len());
        put_size(&mut output, key.len());
        output.extend(key);
        put_size(&mut output, encoded_value.len());
        output.extend(encoded_value);
        Ok(output)
    }

    fn deserialize(&self, value: &[u8]) -> Result<KeyValue<K, V>, SerdeError> {
        let mut reader = Reader::new(value, usize::MAX)?;
        let key_length = reader.read_size(reader.data.len(), "key length")?;
        let key = self
            .key_serde
            .deserialize(reader.read(key_length, "key")?)?;
        let value_length = reader.read_size(reader.data.len(), "value length")?;
        let value = self
            .value_serde
            .deserialize(reader.read(value_length, "value")?)?;
        Ok(KeyValue { key, value })
    }
}

impl<K, V> StreamSerde<KeyValue<K, V>> for StreamKeyValueSerde<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    fn is_key_value(&self) -> bool {
        true
    }

    fn serialize_key(&self, value: &KeyValue<K, V>) -> Result<Option<Vec<u8>>, SerdeError> {
        self.key_serde.serialize(&value.key).map(Some)
    }

    fn serialize_value(&self, value: &KeyValue<K, V>) -> Result<Vec<u8>, SerdeError> {
        self.value_serde.serialize(&value.value)
    }

    fn deserialize_key_value(
        &self,
        key: Option<&[u8]>,
        value: &[u8],
    ) -> Result<KeyValue<K, V>, SerdeError> {
        let key = key.ok_or_else(|| SerdeError::new("key is required", 0))?;
        Ok(KeyValue {
            key: self.key_serde.deserialize(key)?,
            value: self.value_serde.deserialize(value)?,
        })
    }
}

pub fn make_stream_key_value_serde<K, V>(
    key_serde: Arc<dyn Serde<K>>,
    value_serde: Arc<dyn Serde<V>>,
) -> Arc<dyn StreamSerde<KeyValue<K, V>>>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    Arc::new(StreamKeyValueSerde::new(key_serde, value_serde))
}

struct Reader<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(data: &'a [u8], max_total_bytes: usize) -> Result<Self, SerdeError> {
        if data.len() > max_total_bytes {
            return Err(SerdeError::new("serde input exceeds configured limit", 0));
        }
        Ok(Self { data, offset: 0 })
    }

    fn read(&mut self, size: usize, what: &str) -> Result<&'a [u8], SerdeError> {
        let end = self
            .offset
            .checked_add(size)
            .ok_or_else(|| SerdeError::new("serde size overflow", self.offset))?;
        if end > self.data.len() {
            return Err(SerdeError::new(
                format!("serde underflow while reading {what}"),
                self.offset,
            ));
        }
        let result = &self.data[self.offset..end];
        self.offset = end;
        Ok(result)
    }

    fn read_size(&mut self, maximum: usize, what: &str) -> Result<usize, SerdeError> {
        let offset = self.offset;
        let encoded = u64::from_be_bytes(
            self.read(SIZE_BYTES, what)?
                .try_into()
                .expect("size frame has fixed width"),
        );
        let size = usize::try_from(encoded)
            .map_err(|_| SerdeError::new(format!("{what} does not fit usize"), offset))?;
        if size > maximum {
            return Err(SerdeError::new(
                format!("{what} exceeds configured limit"),
                offset,
            ));
        }
        Ok(size)
    }
}

fn put_size(output: &mut Vec<u8>, size: usize) {
    output.extend((size as u64).to_be_bytes());
}

macro_rules! unsigned_serde {
    ($name:ident, $type:ty, $size:expr) => {
        #[derive(Default)]
        pub struct $name;

        impl Serde<$type> for $name {
            fn serialize(&self, value: &$type) -> Result<Vec<u8>, SerdeError> {
                Ok(value.to_be_bytes().to_vec())
            }

            fn deserialize(&self, value: &[u8]) -> Result<$type, SerdeError> {
                let mut reader = Reader::new(value, usize::MAX)?;
                Ok(<$type>::from_be_bytes(
                    reader
                        .read($size, stringify!($type))?
                        .try_into()
                        .expect("primitive frame has fixed width"),
                ))
            }
        }
    };
}

macro_rules! signed_serde {
    ($name:ident, $type:ty, $unsigned:ty, $size:expr, $mask:expr) => {
        #[derive(Default)]
        pub struct $name;

        impl Serde<$type> for $name {
            fn serialize(&self, value: &$type) -> Result<Vec<u8>, SerdeError> {
                Ok(((*value as $unsigned) ^ $mask).to_be_bytes().to_vec())
            }

            fn deserialize(&self, value: &[u8]) -> Result<$type, SerdeError> {
                let mut reader = Reader::new(value, usize::MAX)?;
                let encoded = <$unsigned>::from_be_bytes(
                    reader
                        .read($size, stringify!($type))?
                        .try_into()
                        .expect("primitive frame has fixed width"),
                );
                Ok((encoded ^ $mask) as $type)
            }
        }
    };
}

#[derive(Default)]
pub struct BoolSerde;

impl Serde<bool> for BoolSerde {
    fn serialize(&self, value: &bool) -> Result<Vec<u8>, SerdeError> {
        Ok(vec![u8::from(*value)])
    }

    fn deserialize(&self, value: &[u8]) -> Result<bool, SerdeError> {
        let mut reader = Reader::new(value, usize::MAX)?;
        Ok(reader.read(1, "bool")?[0] != 0)
    }
}

signed_serde!(Int8Serde, i8, u8, 1, 0x80);
unsigned_serde!(UInt8Serde, u8, 1);
signed_serde!(Int16Serde, i16, u16, 2, 0x8000);
unsigned_serde!(UInt16Serde, u16, 2);
signed_serde!(Int32Serde, i32, u32, 4, 0x8000_0000);
unsigned_serde!(UInt32Serde, u32, 4);
signed_serde!(Int64Serde, i64, u64, 8, 0x8000_0000_0000_0000);
unsigned_serde!(UInt64Serde, u64, 8);

pub type IntSerde = Int64Serde;
pub type UIntSerde = UInt64Serde;

#[derive(Default)]
pub struct RuneSerde;

impl Serde<char> for RuneSerde {
    fn serialize(&self, value: &char) -> Result<Vec<u8>, SerdeError> {
        Ok((*value as u32).to_be_bytes().to_vec())
    }

    fn deserialize(&self, value: &[u8]) -> Result<char, SerdeError> {
        let encoded = UInt32Serde.deserialize(value)?;
        char::from_u32(encoded).ok_or_else(|| SerdeError::new("invalid Unicode scalar", 0))
    }
}

#[derive(Default)]
pub struct Float32Serde;

impl Serde<f32> for Float32Serde {
    fn serialize(&self, value: &f32) -> Result<Vec<u8>, SerdeError> {
        Ok(value.to_bits().to_be_bytes().to_vec())
    }

    fn deserialize(&self, value: &[u8]) -> Result<f32, SerdeError> {
        Ok(f32::from_bits(UInt32Serde.deserialize(value)?))
    }
}

#[derive(Default)]
pub struct Float64Serde;

impl Serde<f64> for Float64Serde {
    fn serialize(&self, value: &f64) -> Result<Vec<u8>, SerdeError> {
        Ok(value.to_bits().to_be_bytes().to_vec())
    }

    fn deserialize(&self, value: &[u8]) -> Result<f64, SerdeError> {
        Ok(f64::from_bits(UInt64Serde.deserialize(value)?))
    }
}

#[derive(Default)]
pub struct StringSerde {
    limits: SerdeLimits,
}

impl StringSerde {
    pub fn new(limits: SerdeLimits) -> Self {
        Self { limits }
    }
}

impl Serde<String> for StringSerde {
    fn serialize(&self, value: &String) -> Result<Vec<u8>, SerdeError> {
        if value.len() > self.limits.max_string_bytes {
            return Err(SerdeError::new("string exceeds configured serde limit", 0));
        }
        let mut output = Vec::with_capacity(SIZE_BYTES + value.len());
        put_size(&mut output, value.len());
        output.extend(value.as_bytes());
        Ok(output)
    }

    fn deserialize(&self, value: &[u8]) -> Result<String, SerdeError> {
        let mut reader = Reader::new(value, self.limits.max_total_bytes)?;
        let length = reader.read_size(self.limits.max_string_bytes, "string length")?;
        let offset = reader.offset;
        let bytes = reader.read(length, "string")?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| SerdeError::new("string is not valid UTF-8", offset))
    }
}

#[derive(Default)]
pub struct BytesSerde {
    limits: SerdeLimits,
}

impl BytesSerde {
    pub fn new(limits: SerdeLimits) -> Self {
        Self { limits }
    }
}

impl Serde<Vec<u8>> for BytesSerde {
    fn serialize(&self, value: &Vec<u8>) -> Result<Vec<u8>, SerdeError> {
        if value.len() > self.limits.max_bytes {
            return Err(SerdeError::new("byte sequence exceeds configured limit", 0));
        }
        let mut output = Vec::with_capacity(SIZE_BYTES + value.len());
        put_size(&mut output, value.len());
        output.extend(value);
        Ok(output)
    }

    fn deserialize(&self, value: &[u8]) -> Result<Vec<u8>, SerdeError> {
        let mut reader = Reader::new(value, self.limits.max_total_bytes)?;
        let length = reader.read_size(self.limits.max_bytes, "bytes length")?;
        Ok(reader.read(length, "bytes")?.to_vec())
    }
}

pub struct FixedSizeArraySerde<T, S, const ELEMENT_SIZE: usize> {
    element_serde: S,
    limits: SerdeLimits,
    _value: PhantomData<fn() -> T>,
}

impl<T, S, const ELEMENT_SIZE: usize> Default for FixedSizeArraySerde<T, S, ELEMENT_SIZE>
where
    S: Default,
{
    fn default() -> Self {
        Self {
            element_serde: S::default(),
            limits: SerdeLimits::default(),
            _value: PhantomData,
        }
    }
}

impl<T, S, const ELEMENT_SIZE: usize> FixedSizeArraySerde<T, S, ELEMENT_SIZE>
where
    S: Default,
{
    pub fn new(limits: SerdeLimits) -> Self {
        Self {
            element_serde: S::default(),
            limits,
            _value: PhantomData,
        }
    }
}

impl<T, S, const ELEMENT_SIZE: usize> Serde<Vec<T>> for FixedSizeArraySerde<T, S, ELEMENT_SIZE>
where
    T: Send + Sync + 'static,
    S: Serde<T>,
{
    fn serialize(&self, value: &Vec<T>) -> Result<Vec<u8>, SerdeError> {
        if value.len() > self.limits.max_container_elements {
            return Err(SerdeError::new("array exceeds configured element limit", 0));
        }
        let mut output = Vec::with_capacity(SIZE_BYTES + value.len() * ELEMENT_SIZE);
        put_size(&mut output, value.len());
        for item in value {
            let encoded = self.element_serde.serialize(item)?;
            if encoded.len() != ELEMENT_SIZE {
                return Err(SerdeError::new(
                    "fixed-size serde returned an invalid element size",
                    output.len(),
                ));
            }
            output.extend(encoded);
        }
        Ok(output)
    }

    fn deserialize(&self, value: &[u8]) -> Result<Vec<T>, SerdeError> {
        let mut reader = Reader::new(value, self.limits.max_total_bytes)?;
        let count = reader.read_size(self.limits.max_container_elements, "array count")?;
        let payload_size = count
            .checked_mul(ELEMENT_SIZE)
            .ok_or_else(|| SerdeError::new("array size overflow", reader.offset))?;
        if payload_size > reader.data.len().saturating_sub(reader.offset) {
            return Err(SerdeError::new(
                "fixed-size array payload is truncated",
                reader.offset,
            ));
        }
        let mut result = Vec::with_capacity(count);
        for _ in 0..count {
            result.push(
                self.element_serde
                    .deserialize(reader.read(ELEMENT_SIZE, "array element")?)?,
            );
        }
        Ok(result)
    }
}

pub type BoolArraySerde = FixedSizeArraySerde<bool, BoolSerde, 1>;
pub type Int8ArraySerde = FixedSizeArraySerde<i8, Int8Serde, 1>;
pub type UInt8ArraySerde = FixedSizeArraySerde<u8, UInt8Serde, 1>;
pub type Int16ArraySerde = FixedSizeArraySerde<i16, Int16Serde, 2>;
pub type UInt16ArraySerde = FixedSizeArraySerde<u16, UInt16Serde, 2>;
pub type Int32ArraySerde = FixedSizeArraySerde<i32, Int32Serde, 4>;
pub type UInt32ArraySerde = FixedSizeArraySerde<u32, UInt32Serde, 4>;
pub type Int64ArraySerde = FixedSizeArraySerde<i64, Int64Serde, 8>;
pub type UInt64ArraySerde = FixedSizeArraySerde<u64, UInt64Serde, 8>;
pub type Float32ArraySerde = FixedSizeArraySerde<f32, Float32Serde, 4>;
pub type Float64ArraySerde = FixedSizeArraySerde<f64, Float64Serde, 8>;
pub type IntArraySerde = Int64ArraySerde;
pub type UIntArraySerde = UInt64ArraySerde;

#[derive(Default)]
pub struct StringArraySerde {
    limits: SerdeLimits,
}

impl StringArraySerde {
    pub fn new(limits: SerdeLimits) -> Self {
        Self { limits }
    }
}

impl Serde<Vec<String>> for StringArraySerde {
    fn serialize(&self, value: &Vec<String>) -> Result<Vec<u8>, SerdeError> {
        if value.len() > self.limits.max_container_elements {
            return Err(SerdeError::new("array exceeds configured element limit", 0));
        }
        let string_serde = StringSerde::new(self.limits);
        let mut output = Vec::new();
        put_size(&mut output, value.len());
        for item in value {
            string_serde.serialize_to(&mut output, item)?;
        }
        Ok(output)
    }

    fn deserialize(&self, value: &[u8]) -> Result<Vec<String>, SerdeError> {
        let mut reader = Reader::new(value, self.limits.max_total_bytes)?;
        let count = reader.read_size(self.limits.max_container_elements, "array count")?;
        let mut result = Vec::with_capacity(count);
        for _ in 0..count {
            let length = reader.read_size(self.limits.max_string_bytes, "string length")?;
            let offset = reader.offset;
            result.push(
                String::from_utf8(reader.read(length, "string")?.to_vec())
                    .map_err(|_| SerdeError::new("string is not valid UTF-8", offset))?,
            );
        }
        Ok(result)
    }
}

pub struct ArraySerde<T> {
    element_serde: Arc<dyn Serde<T>>,
    limits: SerdeLimits,
}

impl<T> ArraySerde<T> {
    pub fn new(element_serde: Arc<dyn Serde<T>>) -> Self {
        Self {
            element_serde,
            limits: SerdeLimits::default(),
        }
    }

    pub fn with_limits(element_serde: Arc<dyn Serde<T>>, limits: SerdeLimits) -> Self {
        Self {
            element_serde,
            limits,
        }
    }
}

impl<T> Serde<Vec<T>> for ArraySerde<T>
where
    T: Send + Sync + 'static,
{
    fn serialize(&self, value: &Vec<T>) -> Result<Vec<u8>, SerdeError> {
        if value.len() > self.limits.max_container_elements {
            return Err(SerdeError::new("array exceeds configured element limit", 0));
        }
        let mut output = Vec::new();
        put_size(&mut output, value.len());
        for item in value {
            let encoded = self.element_serde.serialize(item)?;
            put_size(&mut output, encoded.len());
            output.extend(encoded);
        }
        Ok(output)
    }

    fn deserialize(&self, value: &[u8]) -> Result<Vec<T>, SerdeError> {
        let mut reader = Reader::new(value, self.limits.max_total_bytes)?;
        let count = reader.read_size(self.limits.max_container_elements, "array count")?;
        let mut result = Vec::with_capacity(count);
        for _ in 0..count {
            let length = reader.read_size(reader.data.len(), "array element length")?;
            result.push(
                self.element_serde
                    .deserialize(reader.read(length, "array element")?)?,
            );
        }
        Ok(result)
    }
}

pub struct MapSerde<K, V> {
    key_serde: Arc<dyn Serde<Vec<K>>>,
    value_serde: Arc<dyn Serde<Vec<V>>>,
}

impl<K, V> MapSerde<K, V> {
    pub fn new(key_serde: Arc<dyn Serde<Vec<K>>>, value_serde: Arc<dyn Serde<Vec<V>>>) -> Self {
        Self {
            key_serde,
            value_serde,
        }
    }
}

impl<K, V> Serde<HashMap<K, V>> for MapSerde<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn serialize(&self, value: &HashMap<K, V>) -> Result<Vec<u8>, SerdeError> {
        let mut keys = Vec::with_capacity(value.len());
        let mut values = Vec::with_capacity(value.len());
        for (key, value) in value {
            keys.push(key.clone());
            values.push(value.clone());
        }
        let encoded_keys = self.key_serde.serialize(&keys)?;
        let encoded_values = self.value_serde.serialize(&values)?;
        let mut output = Vec::new();
        put_size(&mut output, encoded_keys.len());
        output.extend(encoded_keys);
        put_size(&mut output, encoded_values.len());
        output.extend(encoded_values);
        Ok(output)
    }

    fn deserialize(&self, value: &[u8]) -> Result<HashMap<K, V>, SerdeError> {
        let mut reader = Reader::new(value, usize::MAX)?;
        let keys_length = reader.read_size(reader.data.len(), "map keys length")?;
        let keys = self
            .key_serde
            .deserialize(reader.read(keys_length, "map keys")?)?;
        let values_length = reader.read_size(reader.data.len(), "map values length")?;
        let values = self
            .value_serde
            .deserialize(reader.read(values_length, "map values")?)?;
        if keys.len() != values.len() {
            return Err(SerdeError::new(
                "map key and value counts do not match",
                reader.offset,
            ));
        }
        Ok(keys.into_iter().zip(values).collect())
    }
}

#[derive(Default)]
pub struct StubSerde<T>(PhantomData<fn() -> T>);

impl<T> StubSerde<T> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> Serde<T> for StubSerde<T>
where
    T: Send + Sync,
{
    fn serialize(&self, _value: &T) -> Result<Vec<u8>, SerdeError> {
        Err(SerdeError::new("stub serde cannot serialize", 0))
    }

    fn deserialize(&self, _value: &[u8]) -> Result<T, SerdeError> {
        Err(SerdeError::new("stub serde cannot deserialize", 0))
    }

    fn is_stub(&self) -> bool {
        true
    }
}

#[derive(Default)]
pub struct JsonSerde<T>(PhantomData<fn() -> T>);

impl<T> JsonSerde<T> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> Serde<T> for JsonSerde<T>
where
    T: Serialize + DeserializeOwned + Send + Sync,
{
    fn serialize(&self, value: &T) -> Result<Vec<u8>, SerdeError> {
        serde_json::to_vec(value).map_err(|error| SerdeError::new(error.to_string(), 0))
    }

    fn deserialize(&self, value: &[u8]) -> Result<T, SerdeError> {
        serde_json::from_slice(value).map_err(|error| SerdeError::new(error.to_string(), 0))
    }
}
