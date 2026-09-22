use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::{Arc, RwLock},
};

use super::*;
use crate::runtime::environment::{RuntimeEnvironment, RuntimeError, RuntimeResult};

/// A type-erased serializer whose payload is still checked by Rust's TypeId.
#[derive(Clone)]
pub struct Serializer(Arc<dyn Any + Send + Sync>);

impl Serializer {
    pub fn new<T: Send + Sync + 'static>(serde: Arc<dyn Serde<T>>) -> Self {
        Self(Arc::new(make_stream_serde(serde)))
    }

    pub fn downcast<T: Send + Sync + 'static>(&self) -> Option<Arc<dyn StreamSerde<T>>> {
        self.0.downcast_ref::<Arc<dyn StreamSerde<T>>>().cloned()
    }
}

pub type SerdeProvider = fn(TypeId, &RuntimeEnvironment) -> RuntimeResult<Option<Serializer>>;

#[derive(Default)]
struct RegistryState {
    providers: HashMap<Option<i32>, SerdeProvider>,
    serializers: HashMap<(Option<i32>, TypeId), Serializer>,
}

#[derive(Default)]
pub(crate) struct SerdeRegistry(RwLock<RegistryState>);

impl SerdeRegistry {
    pub(crate) fn set_provider(
        &self,
        service: Option<i32>,
        provider: SerdeProvider,
    ) -> RuntimeResult<()> {
        let mut state = self.0.write().expect("serde registry lock poisoned");
        if state.providers.contains_key(&service)
            || state.serializers.keys().any(|(id, _)| *id == service)
        {
            return Err(RuntimeError::DuplicateResource(
                "service serde provider".to_owned(),
            ));
        }
        state.providers.insert(service, provider);
        Ok(())
    }

    pub(crate) fn get<T: Send + Sync + 'static>(
        &self,
        environment: &RuntimeEnvironment,
    ) -> RuntimeResult<Arc<dyn StreamSerde<T>>> {
        let key = (environment.service_id(), TypeId::of::<T>());
        let provider = {
            let state = self.0.read().expect("serde registry lock poisoned");
            if let Some(serde) = state.serializers.get(&key) {
                return Self::typed::<T>(serde);
            }
            state.providers.get(&key.0).copied()
        };
        // Do not hold the registry lock while running user code or resolving
        // serializers for container elements.
        let selected = match provider {
            Some(provider) => provider(key.1, environment)?,
            None => None,
        };
        let selected = selected
            .or_else(|| default_serializer(key.1))
            .unwrap_or_else(|| Serializer::new::<T>(Arc::new(StubSerde::<T>::new())));
        Self::typed::<T>(&selected)?;
        let mut state = self.0.write().expect("serde registry lock poisoned");
        Self::typed::<T>(state.serializers.entry(key).or_insert(selected))
    }

    pub(crate) fn cache<T: Send + Sync + 'static>(
        &self,
        service: Option<i32>,
        serde: Arc<dyn StreamSerde<T>>,
    ) -> Arc<dyn StreamSerde<T>> {
        let key = (service, TypeId::of::<T>());
        let mut state = self.0.write().expect("serde registry lock poisoned");
        state
            .serializers
            .entry(key)
            .or_insert_with(|| Serializer(Arc::new(serde)))
            .downcast::<T>()
            .expect("cached serde must match its type key")
    }

    fn typed<T: Send + Sync + 'static>(
        serde: &Serializer,
    ) -> RuntimeResult<Arc<dyn StreamSerde<T>>> {
        serde.downcast::<T>().ok_or_else(|| {
            RuntimeError::InvalidConfiguration(format!(
                "serde provider returned an incompatible serializer for {}",
                std::any::type_name::<T>()
            ))
        })
    }
}

fn default_serializer(value_type: TypeId) -> Option<Serializer> {
    macro_rules! scalar_and_array {
        ($type:ty, $serde:expr) => {
            if value_type == TypeId::of::<$type>() {
                return Some(Serializer::new::<$type>(Arc::new($serde)));
            }
            if value_type == TypeId::of::<Vec<$type>>() {
                return Some(Serializer::new::<Vec<$type>>(Arc::new(
                    ArraySerde::<$type>::new(Arc::new($serde)),
                )));
            }
        };
    }
    // Bytes have their existing specialized framing.
    if value_type == TypeId::of::<Vec<u8>>() {
        return Some(Serializer::new::<Vec<u8>>(Arc::new(BytesSerde::new(
            SerdeLimits::default(),
        ))));
    }
    scalar_and_array!(bool, BoolSerde);
    scalar_and_array!(char, RuneSerde);
    scalar_and_array!(i8, Int8Serde);
    scalar_and_array!(i16, Int16Serde);
    scalar_and_array!(i32, Int32Serde);
    scalar_and_array!(i64, Int64Serde);
    scalar_and_array!(u8, UInt8Serde);
    scalar_and_array!(u16, UInt16Serde);
    scalar_and_array!(u32, UInt32Serde);
    scalar_and_array!(u64, UInt64Serde);
    scalar_and_array!(f32, Float32Serde);
    scalar_and_array!(f64, Float64Serde);
    scalar_and_array!(String, StringSerde::new(SerdeLimits::default()));
    None
}
