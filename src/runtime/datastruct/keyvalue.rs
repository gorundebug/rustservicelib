#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct KeyValue<K, V> {
    pub key: K,
    pub value: V,
}
