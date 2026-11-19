//! Native S3 Query boundary acceptance against disposable Compose MinIO.
use std::{sync::{Arc, Mutex, atomic::{AtomicUsize, Ordering}}, time::Duration};
use async_trait::async_trait;
use aws_sdk_s3::{Client, primitives::ByteStream};
use servicelib::{MessageContext, Payload, dataquery::{self, BeginRequest, Collector,
    EndpointHandler, HandlerError, HandlerResult, s3aws}, operators::QueryStream,
    runtime::{common::{Consumer, RuntimeStream}, config::{CallSemantics, QueryStreamConfig, StreamConfig},
    environment::{RuntimeEnvironment, metrics::Metrics}}};
#[derive(Default)]
struct Probe { calls: AtomicUsize, received: AtomicUsize, ends: AtomicUsize, errors: AtomicUsize, bytes: Mutex<Vec<u8>>, keys: Mutex<Vec<String>>, pages: Mutex<Vec<(usize, bool)>> }
struct Handler { probe: Arc<Probe>, fail: bool, wait: bool }
#[async_trait]
impl EndpointHandler<(), (), ()> for Handler {
    async fn begin_request(&self, context: MessageContext, _: &dyn RuntimeStream) -> Result<BeginRequest<()>, HandlerError> {
        Ok(BeginRequest { context, state: (), error: None })
    }
    async fn end_request(&self, _: MessageContext, _: &dyn RuntimeStream, state: Option<()>, result: &HandlerResult, _: &Collector<()>, _: &Collector<()>) {
        assert_eq!(state, Some(()));
        self.probe.ends.fetch_add(1, Ordering::SeqCst);
        self.probe.errors.fetch_add(usize::from(result.is_err()), Ordering::SeqCst);
    }
}
#[async_trait]
impl s3aws::GetObjectHandler<(), String, ()> for Handler {
    async fn build_request(&self, _: MessageContext, _: &mut (), key: Payload<String>, client: &Client) -> Result<aws_sdk_s3::operation::get_object::builders::GetObjectFluentBuilder, HandlerError> {
        Ok(client.get_object().bucket("query-native").key(&*key))
    }
    async fn handle_result(&self, context: MessageContext, _: &mut (), result: &mut aws_sdk_s3::operation::get_object::GetObjectOutput, _: &Collector<()>) -> HandlerResult {
        self.probe.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail { return Err(std::io::Error::other("fixture mapping error").into()); }
        if self.wait { context.cancelled().await; }
        while let Some(bytes) = result.body.next().await {
            let bytes = bytes?;
            assert!(bytes.iter().all(|value| *value == 42));
            self.probe.received.fetch_add(bytes.len(), Ordering::SeqCst);
            // Retain only a bounded sample, never the complete large object.
            let mut sample = self.probe.bytes.lock().unwrap();
            let keep = bytes.len().min((128 * 1024usize).saturating_sub(sample.len()));
            sample.extend_from_slice(&bytes[..keep]);
        }
        Ok(())
    }
}
#[async_trait]
impl s3aws::ListObjectsHandler<(), String, ()> for Handler {
    async fn build_request(&self, _: MessageContext, _: &mut (), prefix: Payload<String>, client: &Client) -> Result<aws_sdk_s3::operation::list_objects_v2::builders::ListObjectsV2FluentBuilder, HandlerError> {
        Ok(client.list_objects_v2().bucket("query-native").prefix(&*prefix).max_keys(1))
    }
    async fn handle_page(&self, _: MessageContext, _: &mut (), page: &mut aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output, _: &Collector<()>) -> HandlerResult {
        self.probe.calls.fetch_add(1, Ordering::SeqCst);
        self.probe.pages.lock().unwrap().push((page.contents().len(), page.is_truncated().unwrap_or(false)));
        self.probe.keys.lock().unwrap().extend(page.contents().iter().filter_map(|o| o.key().map(str::to_owned)));
        if self.fail { return Err(std::io::Error::other("fixture page error").into()); }
        Ok(())
    }
}
fn stream() -> Arc<QueryStream<String, (), ()>> {
    QueryStream::new(&QueryStreamConfig { stream: StreamConfig::new(1, "native S3"), endpoint_id: 1 },
        RuntimeEnvironment::with_metrics(CallSemantics::FunctionCall, Metrics::noop())).unwrap()
}
#[tokio::test]
#[ignore = "requires disposable Compose MinIO and SA_QUERY_DISPOSABLE=1"]
async fn native_empty_missing_pagination_mapper_errors_and_cancel() {
    assert_eq!(std::env::var("SA_QUERY_DISPOSABLE").as_deref(), Ok("1"));
    let native = aws_sdk_s3::Config::builder()
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .credentials_provider(aws_sdk_s3::config::Credentials::new(
            std::env::var("AWS_ACCESS_KEY_ID").unwrap(), std::env::var("AWS_SECRET_ACCESS_KEY").unwrap(), None, None, "disposable-compose"))
        .endpoint_url(std::env::var("OBJECTS_ADDRESS").unwrap()).force_path_style(true).build();
    let client = s3aws::S3Client::new(native);
    client.native().create_bucket().bucket("query-native").send().await.unwrap();
    let payload = vec![42u8; 16 * 1024 * 1024];
    for (key, bytes) in [("objects/empty", vec![]), ("objects/large", payload.clone()), ("objects/third", vec![1])] {
        client.native().put_object().bucket("query-native").key(key).body(ByteStream::from(bytes)).send().await.unwrap();
    }
    for (key, fail, wait, calls, errors) in [
        ("objects/empty", false, false, 1, 0), ("objects/large", false, false, 1, 0),
        ("absent", false, false, 0, 1), ("objects/large", true, false, 1, 1),
        ("objects/large", false, true, 1, 1),
    ] {
        let probe = Arc::new(Probe::default());
        let stream = stream();
        let consumer = dataquery::make_endpoint_consumer(&stream, Handler { probe: probe.clone(), fail, wait }, s3aws::GetObjectOperation::new(client.clone())).unwrap();
        let context = MessageContext::new().with_timeout_limit(if wait { Duration::from_millis(100) } else { Duration::from_secs(2) });
        tokio::time::timeout(Duration::from_secs(3), consumer.consume(context, Payload::new(key.into()))).await.unwrap();
        assert_eq!(probe.calls.load(Ordering::SeqCst), calls, "{key}");
        assert_eq!(probe.ends.load(Ordering::SeqCst), 1);
        assert_eq!(probe.errors.load(Ordering::SeqCst), errors);
        if errors == 0 {
            let expected = if key.ends_with("empty") { 0 } else { payload.len() };
            assert_eq!(probe.received.load(Ordering::SeqCst), expected);
            assert_eq!(probe.bytes.lock().unwrap().len(), expected.min(128 * 1024));
        }
        // Subsequent request must still work after an unread/cancelled body.
        client.native().head_object().bucket("query-native").key("objects/large").send().await.unwrap();
    }
    for (prefix, fail, expected_pages, expected_keys) in [("objects/", false, 3, 3), ("objects/", true, 1, 1), ("missing/", false, 1, 0)] {
        let probe = Arc::new(Probe::default());
        let stream = stream();
        let consumer = dataquery::make_endpoint_consumer(&stream, Handler { probe: probe.clone(), fail, wait: false }, s3aws::ListObjectsOperation::new(client.clone())).unwrap();
        consumer.consume(MessageContext::new().with_timeout_limit(Duration::from_secs(2)), Payload::new(prefix.into())).await;
        let pages = probe.pages.lock().unwrap();
        if expected_keys == 3 {
            // Some compatible servers return an empty final page after a full
            // last page. Validate records and completion, not page packing.
            assert_eq!(pages.iter().filter(|(count, _)| *count > 0).count(), expected_pages);
            assert!(pages.len() == 3 || (pages.len() == 4 && pages.last() == Some(&(0, false))));
            assert!(!pages.last().unwrap().1);
            assert_eq!(*probe.keys.lock().unwrap(), ["objects/empty", "objects/large", "objects/third"]);
        } else {
            assert_eq!(probe.calls.load(Ordering::SeqCst), expected_pages);
        }
        assert_eq!(probe.keys.lock().unwrap().len(), expected_keys);
        assert_eq!(probe.ends.load(Ordering::SeqCst), 1);
        assert_eq!(probe.errors.load(Ordering::SeqCst), usize::from(fail));
    }
}

#[tokio::test]
#[ignore = "requires disposable TLS fixture and explicit expected trust outcome"]
async fn native_tls_verifies_server_certificate() {
    assert_eq!(std::env::var("SA_QUERY_DISPOSABLE").as_deref(), Ok("1"));
    let trusted = match std::env::var("SA_QUERY_TLS_TRUSTED").as_deref() {
        Ok("1") => true,
        Ok("0") => false,
        _ => panic!("explicit trust outcome required"),
    };
    let native = aws_sdk_s3::Config::builder()
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .credentials_provider(aws_sdk_s3::config::Credentials::new(
            "fixture", "fixture", None, None, "disposable-tls"))
        .endpoint_url("https://localhost:9443").force_path_style(true).build();
    let connector = s3aws::S3Client::new(native);
    let result = tokio::time::timeout(Duration::from_secs(10),
        connector.native().head_object().bucket("bucket").key("key").send()).await
        .expect("native TLS request timed out");
    if trusted {
        assert_eq!(result.expect("trusted CA rejected").content_length(), Some(0));
    } else {
        let error = result.expect_err("untrusted CA accepted");
        let diagnostic = format!("{error:?}");
        assert!(diagnostic.contains("UnknownIssuer") || diagnostic.to_lowercase().contains("certificate"),
                "expected certificate verification failure: {diagnostic}");
    }
}
