use std::{convert::Infallible, sync::Arc, time::Duration};

use axum::{Router, body::Body, routing::get};
use futures::StreamExt;
use servicelib::{
    MessageContext,
    datasink::http::{Client, Request, ReqwestClient},
};
use tokio::sync::Notify;
use tokio::io::AsyncReadExt;

enum Finish {
    Cancel,
    Deadline,
    Body,
    HeadersOnly,
    PartialRead,
}

async fn slow_body(finish: Finish) {
    let body_started = Arc::new(Notify::new());
    let release_body = Arc::new(Notify::new());
    let app = Router::new().route(
        "/slow",
        get({
            let body_started = body_started.clone();
            let release_body = release_body.clone();
            move || {
                let body_started = body_started.clone();
                let release_body = release_body.clone();
                async move {
                    let first = futures::stream::once(async {
                        Ok::<_, Infallible>("first")
                    });
                    let last = futures::stream::once(async move {
                        body_started.notify_one();
                        release_body.notified().await;
                        Ok::<_, Infallible>("last")
                    });
                    Body::from_stream(first.chain(last))
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let context = match finish {
        Finish::Deadline => MessageContext::with_timeout(Duration::from_secs(1)),
        _ => MessageContext::new(),
    };
    let request_context = context.clone();
    let request = tokio::spawn(async move {
        ReqwestClient::default()
            .perform(
                request_context,
                Request {
                    method: "GET".to_owned(),
                    url: format!("http://{address}/slow"),
                    ..Default::default()
                },
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), body_started.notified())
        .await
        .expect("server did not start the response body");
    let mut response = tokio::time::timeout(Duration::from_secs(1), request).await
        .expect("headers were withheld until the whole body finished").unwrap().unwrap();
    assert_eq!(response.status, 200);
    if matches!(finish, Finish::HeadersOnly | Finish::PartialRead) {
        if matches!(finish, Finish::PartialRead) {
            let mut first = [0_u8; 5];
            response.body.read_exact(&mut first).await.unwrap();
            assert_eq!(&first, b"first");
        }
        response.body.close();
        assert!(response.body.read(&mut [0_u8; 1]).await.is_err());
        server.abort();
        let _ = server.await;
        return;
    }
    let mut request = tokio::spawn(async move { response.body.bytes().await });
    // Only explicit body consumption waits for the final chunk.
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut request)
            .await
            .is_err()
    );
    match finish {
        Finish::Cancel => context.cancel(),
        Finish::Deadline => {}
        Finish::Body => release_body.notify_one(),
        Finish::HeadersOnly | Finish::PartialRead => unreachable!(),
    }
    let completed = tokio::time::timeout(Duration::from_secs(3), &mut request).await;
    // Clean up even if cancellation regresses and the request stays pending.
    request.abort();
    release_body.notify_one();
    server.abort();
    let result = completed.expect("body read ignored cancellation or deadline").unwrap();
    match finish {
        Finish::Cancel => assert_eq!(
            result.unwrap_err().to_string(),
            "HTTP request context cancelled"
        ),
        Finish::Deadline => assert!(result.is_err()),
        Finish::Body => assert_eq!(result.unwrap(), b"firstlast"),
        Finish::HeadersOnly | Finish::PartialRead => unreachable!(),
    }
}

#[tokio::test]
async fn cancellation_interrupts_response_body_after_headers() {
    slow_body(Finish::Cancel).await;
}

#[tokio::test]
async fn deadline_interrupts_response_body_after_headers() {
    slow_body(Finish::Deadline).await;
}

#[tokio::test]
async fn explicit_body_read_waits_for_the_complete_response_body() {
    slow_body(Finish::Body).await;
}

#[tokio::test]
async fn headers_are_available_without_reading_the_body() { slow_body(Finish::HeadersOnly).await; }

#[tokio::test]
async fn partial_read_does_not_wait_for_remaining_body() { slow_body(Finish::PartialRead).await; }
