use std::time::Duration;
use crate::{
    MessageContext,
    runtime::{nats::{bounded, Connection}, config::NatsDataConnectorConfig},
};

#[tokio::test]
async fn cancellation_at_completion_boundary() {
    let owner = MessageContext::new();
    let result = bounded(&owner, Duration::from_secs(1), async {
        owner.cancel();
        Ok(())
    }).await;
    assert!(result.is_err(), "late success after cancellation");
}

#[tokio::test]
async fn native_startup_gate() {
    let Ok(url) = std::env::var("NATS_STARTUP_URL") else { return };
    let mode = std::env::var("NATS_STARTUP_MODE").unwrap();
    // The existing native reconnect callback also delays the first attempt by
    // 250 ms. Exercise a real socket handshake without changing that policy.
    let owner = if mode == "deadline" {
        MessageContext::with_timeout(Duration::from_millis(350))
    } else { MessageContext::new() };
    if mode == "pre-cancel" { owner.cancel(); }
    let cancel = owner.clone();
    let timer = if mode == "cancel" {
        Some(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            cancel.cancel();
        }))
    } else { None };
    let config = NatsDataConnectorConfig {
        servers: vec![url],
        connect_timeout_ms: if mode == "configured-timeout" { 350 } else { 1200 },
        ..Default::default()
    };
    let (tx, _rx) = tokio::sync::mpsc::channel(8);
    let begin = std::time::Instant::now();
    let result = Connection::connect(&config, &owner, tx).await;
    assert_eq!(result.is_err(), mode != "control");
    if let Ok(connection) = result {
        connection.drain(
            &MessageContext::with_timeout(Duration::from_millis(200)),
            Duration::from_millis(200),
        ).await.unwrap();
    }
    assert!(begin.elapsed() < Duration::from_millis(700));
    if let Some(timer) = timer { timer.await.unwrap(); }
}
