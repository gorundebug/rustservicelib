//! Constructor acceptance only: no credentials, network requests or S3 server.
use aws_sdk_s3::{Client, config::{BehaviorVersion, Region, retry::RetryConfig}};
use servicelib::dataquery::s3aws::S3Client;

fn client(attempts: u32) -> Client {
    Client::from_conf(aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("eu-west-1"))
        .retry_config(RetryConfig::standard().with_max_attempts(attempts))
        .build())
}

#[tokio::test]
async fn maker_client_must_not_retry_query_operations() {
    assert!(S3Client::from_client(client(3), None).is_err());
    let configured = S3Client::from_client(client(1), None)
        .expect("single-attempt maker client must be accepted");
    assert_eq!(configured.native().config().region().unwrap().as_ref(), "eu-west-1");
}
