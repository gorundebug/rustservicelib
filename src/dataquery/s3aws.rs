//! Native AWS S3 Query operations. No DataSink inheritance or background listing.
//! Result/body references are scoped to the mapper; drop releases unread bodies
//! before EndRequest. The application may stream GetObject.body without collecting it.
use async_trait::async_trait;
use aws_sdk_s3::{Client, config::retry::RetryConfig};
use super::{Collector, HandlerError, HandlerResult, Operation};
use crate::runtime::{common::{MessageContext, Payload}};

fn cancelled() -> HandlerError {
    std::io::Error::new(std::io::ErrorKind::Interrupted, "S3 Query cancelled").into()
}

/// All operations share one native client. Credentials, region, endpoint and TLS
/// come from the connector's AWS configuration. Retries are disabled centrally:
/// writes may have unknown outcomes and partially emitted reads must not replay.
#[derive(Clone)]
pub struct S3Client {
    client: Client,
    operation_timeout: Option<std::time::Duration>,
}
impl S3Client {
    /// Use the client configured by an infrastructure maker. Reject automatic
    /// retries rather than silently replaying writes with an unknown outcome.
    pub fn from_client(client: Client, operation_timeout: Option<std::time::Duration>)
        -> crate::runtime::environment::RuntimeResult<Self> {
        if client.config().retry_config().map(|retry| retry.max_attempts()) != Some(1) {
            return Err(crate::runtime::environment::RuntimeError::InvalidConfiguration(
                "S3 Query requires a client configured for one attempt".into()));
        }
        Ok(Self { client, operation_timeout })
    }

    pub fn new(config: aws_sdk_s3::Config) -> Self {
        let config = config.to_builder().retry_config(RetryConfig::disabled()).build();
        Self { client: Client::from_conf(config), operation_timeout: None }
    }
    /// Apply runtime connector settings to the native AWS configuration. The
    /// caller supplies the standard credential provider chain; no credentials
    /// are copied into graph configuration or logs.
    pub fn configured(native: aws_sdk_s3::Config,
        config: &crate::runtime::config::S3AWSDataConnectorConfig,
    ) -> crate::runtime::environment::RuntimeResult<Self> {
        use std::time::Duration;
        use aws_sdk_s3::config::{Region, timeout::TimeoutConfig};
        config.validate()?;
        let operation_timeout = Duration::from_millis(config.operation_timeout_ms);
        let mut builder = native.to_builder()
            .region(Region::new(config.region.clone()))
            .force_path_style(config.force_path_style)
            .retry_config(RetryConfig::disabled())
            .timeout_config(TimeoutConfig::builder()
                .connect_timeout(Duration::from_millis(config.connect_timeout_ms))
                .operation_timeout(operation_timeout).build());
        if let Some(address) = &config.address {
            builder = builder.endpoint_url(address);
        }
        Ok(Self { client: Client::from_conf(builder.build()),
            operation_timeout: Some(operation_timeout) })
    }
    pub fn native(&self) -> &Client { &self.client }

    fn request_context(&self, context: MessageContext) -> MessageContext {
        match self.operation_timeout {
            Some(timeout) => context.with_timeout_limit(timeout),
            None => context,
        }
    }
}

macro_rules! single_operation {
    ($handler:ident, $operation:ident, $module:ident, $builder:ident, $output:ident) => {
        #[async_trait]
        pub trait $handler<HandlerState, T, R>: Send + Sync
        where HandlerState: Send + 'static, T: Send + Sync + 'static, R: Send + Sync + 'static {
            async fn build_request(&self, context: MessageContext, state: &mut HandlerState,
                value: Payload<T>, client: &Client)
                -> Result<aws_sdk_s3::operation::$module::builders::$builder, HandlerError>;
            async fn handle_result(&self, context: MessageContext, state: &mut HandlerState,
                result: &mut aws_sdk_s3::operation::$module::$output,
                out: &Collector<R>) -> HandlerResult;
        }
        pub struct $operation { client: S3Client }
        impl $operation { pub fn new(client: S3Client) -> Self { Self { client } } }
        #[async_trait]
        impl<HandlerState, T, R, H> Operation<HandlerState, T, R, H> for $operation
        where HandlerState: Send + 'static, T: Send + Sync + 'static,
            R: Send + Sync + 'static, H: $handler<HandlerState, T, R> {
            async fn execute(&self, context: MessageContext, state: &mut HandlerState,
                value: Payload<T>, handler: &H, out: &Collector<R>) -> HandlerResult {
                let context = self.client.request_context(context);
                let request = tokio::select! {
                    biased;
                    _ = context.cancelled() => return Err(cancelled()),
                    request = handler.build_request(context.clone(), state, value, self.client.native()) => request?,
                };
                let mut result = tokio::select! {
                    biased;
                    _ = context.cancelled() => return Err(cancelled()),
                    result = request.send() => result?,
                };
                let mapped = tokio::select! {
                    biased;
                    _ = context.cancelled() => Err(cancelled()),
                    result = handler.handle_result(context.clone(), state, &mut result, out) => result,
                };
                // Includes a GetObject ByteStream on success, error and cancellation.
                drop(result);
                mapped?;
                if context.is_cancelled() { return Err(cancelled()); }
                Ok(())
            }
        }
    }
}
single_operation!(GetObjectHandler, GetObjectOperation, get_object, GetObjectFluentBuilder, GetObjectOutput);
single_operation!(HeadObjectHandler, HeadObjectOperation, head_object, HeadObjectFluentBuilder, HeadObjectOutput);
single_operation!(PutObjectHandler, PutObjectOperation, put_object, PutObjectFluentBuilder, PutObjectOutput);
single_operation!(DeleteObjectHandler, DeleteObjectOperation, delete_object, DeleteObjectFluentBuilder, DeleteObjectOutput);

#[async_trait]
pub trait ListObjectsHandler<HandlerState, T, R>: Send + Sync
where HandlerState: Send + 'static, T: Send + Sync + 'static, R: Send + Sync + 'static {
    async fn build_request(&self, context: MessageContext, state: &mut HandlerState,
        value: Payload<T>, client: &Client)
        -> Result<aws_sdk_s3::operation::list_objects_v2::builders::ListObjectsV2FluentBuilder, HandlerError>;
    async fn handle_page(&self, context: MessageContext, state: &mut HandlerState,
        page: &mut aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output,
        out: &Collector<R>) -> HandlerResult;
}
pub struct ListObjectsOperation { client: S3Client }
impl ListObjectsOperation { pub fn new(client: S3Client) -> Self { Self { client } } }
#[async_trait]
impl<HandlerState, T, R, H> Operation<HandlerState, T, R, H> for ListObjectsOperation
where HandlerState: Send + 'static, T: Send + Sync + 'static,
    R: Send + Sync + 'static, H: ListObjectsHandler<HandlerState, T, R> {
    async fn execute(&self, context: MessageContext, state: &mut HandlerState,
        value: Payload<T>, handler: &H, out: &Collector<R>) -> HandlerResult {
        // One deadline covers every page and its mapper, including body reads.
        let context = self.client.request_context(context);
        let request = tokio::select! {
            biased;
            _ = context.cancelled() => return Err(cancelled()),
            request = handler.build_request(context.clone(), state, value, self.client.native()) => request?,
        };
        let mut previous_token = request.get_continuation_token().clone();
        let mut pages = request.into_paginator().send();
        loop {
            let page = tokio::select! {
                biased;
                _ = context.cancelled() => return Err(cancelled()),
                page = pages.next() => page,
            };
            let Some(page) = page else { break; };
            let mut page = page?;
            // Preserve protocol metadata before the application mutates its page.
            let truncated = page.is_truncated().unwrap_or(false);
            let next_token = page.next_continuation_token().map(str::to_owned);
            tokio::select! {
                biased;
                _ = context.cancelled() => return Err(cancelled()),
                result = handler.handle_page(context.clone(), state, &mut page, out) => result?,
            }
            if truncated {
                if next_token.as_deref().is_none_or(str::is_empty) || next_token == previous_token {
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData,
                        "S3 listing did not advance its continuation token").into());
                }
                previous_token = next_token;
            }
        }
        if context.is_cancelled() { return Err(cancelled()); }
        Ok(())
    }
}
