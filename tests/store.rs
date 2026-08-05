use std::time::Duration;

use servicelib::{
    MessageContext,
    runtime::{
        environment::RuntimeError,
        store::{RotatingMap, Storage},
    },
};

#[test]
fn rotating_map_reclaims_capacity_without_expiring_values() {
    let map = RotatingMap::new(Duration::from_secs(60));
    for key in 0..1_000 {
        map.set(key, key).unwrap();
    }
    for key in 10..1_000 {
        map.pop(&key);
    }
    map.rotate();

    for key in 0..10 {
        assert_eq!(map.get(&key), Some(key));
    }
}

#[test]
fn rotating_map_get_or_create_and_conditional_pop_are_atomic() {
    let map = RotatingMap::new(Duration::from_secs(60));
    let (value, loaded) = map.get_or_create("stream".to_owned(), || 7);
    assert_eq!((value, loaded), (7, false));

    let (value, loaded) = map.get_or_create("stream".to_owned(), || 9);
    assert_eq!((value, loaded), (7, true));
    assert_eq!(map.pop_if("stream", |value| *value == 9), None);
    assert_eq!(map.get("stream"), Some(7));
    assert_eq!(map.pop_if("stream", |value| *value == 7), Some(7));
    assert_eq!(map.get("stream"), None);
}

#[tokio::test]
async fn rotating_map_rejects_duplicates_and_obeys_lifecycle() {
    let map = RotatingMap::new(Duration::from_millis(1));
    map.set(1, "one").unwrap();
    assert!(matches!(
        map.set(1, "other"),
        Err(RuntimeError::DuplicateKey)
    ));
    map.start(MessageContext::new()).await.unwrap();
    assert!(matches!(
        map.start(MessageContext::new()).await,
        Err(RuntimeError::ResourceAlreadyStarted(_))
    ));
    tokio::time::sleep(Duration::from_millis(2)).await;
    assert_eq!(map.get(&1), Some("one"));
    map.stop(MessageContext::new()).await;
}
