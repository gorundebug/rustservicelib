use std::collections::HashMap;

// The caller retains the existing synchronization across registration and removal.
pub(super) enum CallbackStore<C> {
    Inline(Option<(String, C)>),
    Many(HashMap<String, C>),
}

impl<C> CallbackStore<C> {
    pub(super) fn new() -> Self {
        Self::Inline(None)
    }

    pub(super) fn insert(&mut self, id: String, callback: C) -> Option<C> {
        match self {
            Self::Inline(slot) => match slot {
                None => {
                    *slot = Some((id, callback));
                    None
                }
                Some((key, value)) if *key == id => Some(std::mem::replace(value, callback)),
                Some(_) => {
                    let mut many = HashMap::with_capacity(2);
                    let (key, value) = slot.take().expect("occupied inline callback");
                    many.insert(key, value);
                    many.insert(id, callback);
                    *self = Self::Many(many);
                    None
                }
            },
            Self::Many(many) => many.insert(id, callback),
        }
    }

    pub(super) fn get(&self, id: &str) -> Option<&C> {
        match self {
            Self::Inline(Some((key, value))) if key == id => Some(value),
            Self::Inline(_) => None,
            Self::Many(many) => many.get(id),
        }
    }

    pub(super) fn remove(&mut self, id: &str) -> Option<C> {
        match self {
            Self::Inline(slot) if slot.as_ref().is_some_and(|(key, _)| key == id) => {
                slot.take().map(|(_, value)| value)
            }
            Self::Inline(_) => None,
            Self::Many(many) => many.remove(id),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn operations_match_hash_map() {
        let mut store = CallbackStore::new();
        let mut reference = HashMap::new();
        let mut seed = 13_u64;
        for value in 0..10_000 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let id = if seed % 7 == 0 {
                String::new()
            } else {
                (seed % 11).to_string()
            };
            match seed >> 32 & 3 {
                0 | 1 => assert_eq!(store.insert(id.clone(), value), reference.insert(id, value)),
                2 => assert_eq!(store.remove(&id), reference.remove(&id)),
                _ => assert_eq!(store.get(&id), reference.get(&id)),
            }
        }
    }

    #[test]
    fn single_id_stays_inline_and_lookup_survives_removal() {
        let mut store = CallbackStore::new();
        store.insert(String::new(), Arc::new(1));
        store.insert(String::new(), Arc::new(2));
        assert!(matches!(store, CallbackStore::Inline(_)));
        let retained = store.get("").unwrap().clone();
        assert_eq!(*store.remove("").unwrap(), 2);
        assert!(store.remove("").is_none());
        assert_eq!(*retained, 2);
        store.insert("a".into(), Arc::new(3));
        store.insert("b".into(), Arc::new(4));
        assert!(matches!(store, CallbackStore::Many(_)));
        assert_eq!(**store.get("a").unwrap(), 3);
    }
}
