#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyValue<K, V> {
    pub key: K,
    pub value: V,
}
