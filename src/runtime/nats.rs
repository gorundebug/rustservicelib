//! Shared native transport. All queues are bounded; no publisher task is spawned
//! before admission. Broker ACKs remain application-owned.
use std::{future::Future, sync::{Arc, atomic::{AtomicBool, Ordering}}, time::Duration};
use async_nats::{Client, ConnectOptions, Event, jetstream::{self, consumer::{self, PullConsumer}, context::{ContextBuilder, GetStreamErrorKind, ConsumerInfoErrorKind}}};
use tokio::sync::mpsc;
use super::{common::MessageContext, config::{NatsDataConnectorConfig, NatsEndpointConfig}, environment::{RuntimeError, RuntimeResult}};
pub const CAPACITY: usize = 64;
pub const MAX_MESSAGE_BYTES: usize = 1024 * 1024;
pub const MAX_PENDING_BYTES: usize = 8 * 1024 * 1024;
pub fn transport(error: impl std::fmt::Display) -> RuntimeError { RuntimeError::Transport(error.to_string()) }
pub async fn bounded<T>(context: &MessageContext, timeout: Duration, future: impl Future<Output=RuntimeResult<T>>) -> RuntimeResult<T> {
    tokio::select! { biased; _ = context.cancelled() => Err(RuntimeError::ContextCancelled), result = tokio::time::timeout(timeout, future) => {
        let value=result.map_err(|_| transport("NATS operation timed out"))?;
        if context.is_cancelled() { Err(RuntimeError::ContextCancelled) } else { value }
    } }
}
pub struct Connection { pub client: Client, pub js: jetstream::Context, closed:tokio_util::sync::CancellationToken, draining:Arc<AtomicBool> }
impl Connection {
    pub async fn connect(config: &NatsDataConnectorConfig, context: &MessageContext, errors: mpsc::Sender<RuntimeError>) -> RuntimeResult<Arc<Self>> {
        if context.is_cancelled() { return Err(RuntimeError::ContextCancelled); }
        config.validate()?;
        let closed=tokio_util::sync::CancellationToken::new();let on_closed=closed.clone();
        let draining=Arc::new(AtomicBool::new(false));let on_draining=draining.clone();
        let mut options = ConnectOptions::new().connection_timeout(config.connect_timeout())
            .request_timeout(Some(config.operation_timeout())).subscription_capacity(CAPACITY).client_capacity(CAPACITY)
            .max_reconnects(60).reconnect_delay_callback(|_| Duration::from_millis(250))
            .require_tls(config.tls_enabled || config.servers.iter().any(|s| s.starts_with("tls://")))
            .event_callback(move |event| { let errors=errors.clone();let closed=on_closed.clone();let draining=on_draining.clone(); async move {
                if matches!(event,Event::Closed){
                    closed.cancel();
                    // Native drain ends with Closed; only an unsolicited closure is an error.
                    if draining.load(Ordering::Acquire){return;}
                }
                match event { Event::SlowConsumer(_) | Event::ServerError(_) | Event::ClientError(_) | Event::Closed => {
                    // Coalesce transport error bursts; the callback never blocks reconnection.
                    let _ = errors.try_send(transport(event));
                }, _ => {} }
            }});
        if !config.token.is_empty() { options=options.token(config.token.clone()); }
        if !config.username.is_empty() { options=options.user_and_password(config.username.clone(),config.password.clone()); }
        if !config.credentials_file.is_empty() { options=bounded(context,config.connect_timeout(),async { options.credentials_file(&config.credentials_file).await.map_err(|_| transport("cannot read NATS credentials file")) }).await?; }
        if !config.tls_server_name.is_empty() { options=options.tls_client_config(tls_config(config)?); }
        else {
            if !config.tls_ca_file.is_empty() { options=options.add_root_certificates(config.tls_ca_file.clone().into()); }
            if !config.tls_cert_file.is_empty() { options=options.add_client_certificate(config.tls_cert_file.clone().into(),config.tls_key_file.clone().into()); }
        }
        let client=bounded(context,config.connect_timeout(),async { options.connect(config.servers.clone()).await.map_err(|_| transport("NATS connection failed")) }).await?;
        let js=ContextBuilder::new().timeout(config.operation_timeout()).ack_timeout(config.operation_timeout()).max_ack_inflight(CAPACITY).backpressure_on_inflight(true).build(client.clone());
        Ok(Arc::new(Self {client,js,closed,draining}))
    }
    pub async fn drain(&self, context: &MessageContext, timeout:Duration) -> RuntimeResult<()> {
        bounded(context,timeout,async {
            self.draining.store(true,Ordering::Release);
            self.client.drain().await.map_err(transport)?;
            self.closed.cancelled().await;
            Ok(())
        }).await
    }
    pub async fn stream(&self, endpoint:&NatsEndpointConfig) -> RuntimeResult<jetstream::stream::Stream> {
        let stream=match self.js.get_stream(&endpoint.stream_name).await {
            Ok(stream)=>stream,
            Err(error) if endpoint.create_stream && matches!(error.kind(),GetStreamErrorKind::JetStream(ref e) if e.code()==404)=> {
                // Create never updates an existing stream. A concurrent creator is
                // handled by re-reading and validating the resulting resource.
                match self.js.create_stream(jetstream::stream::Config {name:endpoint.stream_name.clone(),subjects:vec![endpoint.subject.clone()], ..Default::default()}).await {
                    Ok(stream)=>stream, Err(_)=>self.js.get_stream(&endpoint.stream_name).await.map_err(transport)?
                }
            }, Err(error)=>return Err(transport(error))
        };
        if !stream.cached_info().config.subjects.iter().any(|s| covers(s,&endpoint.subject)) { return Err(RuntimeError::InvalidConfiguration("JetStream stream does not cover endpoint subject".into())); }
        Ok(stream)
    }
    pub async fn consumer(&self,endpoint:&NatsEndpointConfig)->RuntimeResult<PullConsumer> {
        let stream=self.stream(endpoint).await?;
        let consumer=match stream.get_consumer::<consumer::pull::Config>(&endpoint.durable_name).await {
            Ok(c)=>c,
            Err(e) if endpoint.create_consumer && e.downcast_ref::<jetstream::context::ConsumerInfoError>().is_some_and(|e| matches!(e.kind(),ConsumerInfoErrorKind::NotFound))=> {
                match stream.create_consumer_strict(consumer::pull::Config {durable_name:Some(endpoint.durable_name.clone()),filter_subject:endpoint.subject.clone(),ack_policy:consumer::AckPolicy::Explicit,..Default::default()}).await {
                    Ok(c)=>c,Err(_)=>stream.get_consumer::<consumer::pull::Config>(&endpoint.durable_name).await.map_err(transport)?
                }
            },Err(e)=>return Err(transport(e))
        };
        let c=&consumer.cached_info().config;
        if c.ack_policy!=consumer::AckPolicy::Explicit || c.deliver_subject.is_some() || c.filter_subject!=endpoint.subject || !c.filter_subjects.is_empty() {
            return Err(RuntimeError::InvalidConfiguration("incompatible JetStream consumer: requires pull, explicit ACK, exact endpoint filter".into()));
        }
        Ok(consumer)
    }
}
// Subject-set containment, not string prefix matching. `>` requires a token.
pub fn covers(pattern:&str,subject:&str)->bool {
    let p:Vec<_>=pattern.split('.').collect(); let s:Vec<_>=subject.split('.').collect();
    for (i,token) in p.iter().enumerate() {
        if *token==">" { return i<s.len(); }
        let Some(v)=s.get(i) else {return false};
        if *v==">" || (*token!="*" && token!=v) {return false;}
    }
    p.len()==s.len()
}

// Keep certificate chain and signature verification intact while overriding
// the verification name. Never install an accept-all verifier.
#[derive(Debug)]
struct NamedVerifier { inner:Arc<dyn async_nats::rustls::client::danger::ServerCertVerifier>, name:async_nats::rustls::pki_types::ServerName<'static> }
impl async_nats::rustls::client::danger::ServerCertVerifier for NamedVerifier {
    fn verify_server_cert(&self,end:&async_nats::rustls::pki_types::CertificateDer<'_>,chain:&[async_nats::rustls::pki_types::CertificateDer<'_>],_:&async_nats::rustls::pki_types::ServerName<'_>,ocsp:&[u8],now:async_nats::rustls::pki_types::UnixTime)->Result<async_nats::rustls::client::danger::ServerCertVerified,async_nats::rustls::Error>{ self.inner.verify_server_cert(end,chain,&self.name,ocsp,now) }
    fn verify_tls12_signature(&self,message:&[u8],cert:&async_nats::rustls::pki_types::CertificateDer<'_>,dss:&async_nats::rustls::DigitallySignedStruct)->Result<async_nats::rustls::client::danger::HandshakeSignatureValid,async_nats::rustls::Error>{self.inner.verify_tls12_signature(message,cert,dss)}
    fn verify_tls13_signature(&self,message:&[u8],cert:&async_nats::rustls::pki_types::CertificateDer<'_>,dss:&async_nats::rustls::DigitallySignedStruct)->Result<async_nats::rustls::client::danger::HandshakeSignatureValid,async_nats::rustls::Error>{self.inner.verify_tls13_signature(message,cert,dss)}
    fn supported_verify_schemes(&self)->Vec<async_nats::rustls::SignatureScheme>{self.inner.supported_verify_schemes()}
}
fn tls_config(config:&NatsDataConnectorConfig)->RuntimeResult<async_nats::rustls::ClientConfig>{
    use async_nats::rustls::{self,pki_types::{pem::PemObject,CertificateDer,PrivateKeyDer,ServerName}};
    let mut roots=rustls::RootCertStore::empty();
    if config.tls_ca_file.is_empty(){for cert in rustls_native_certs::load_native_certs().certs {roots.add(cert).map_err(transport)?;}}
    else {for cert in CertificateDer::pem_file_iter(&config.tls_ca_file).map_err(transport)? { roots.add(cert.map_err(transport)?).map_err(transport)?; }}
    let provider=Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let verifier=rustls::client::WebPkiServerVerifier::builder_with_provider(Arc::new(roots),provider.clone()).build().map_err(transport)?;
    let builder=rustls::ClientConfig::builder_with_provider(provider).with_safe_default_protocol_versions().map_err(transport)?.dangerous().with_custom_certificate_verifier(Arc::new(NamedVerifier {inner:verifier,name:ServerName::try_from(config.tls_server_name.clone()).map_err(transport)?}));
    if config.tls_cert_file.is_empty(){Ok(builder.with_no_client_auth())}else{
        let certs=CertificateDer::pem_file_iter(&config.tls_cert_file).map_err(transport)?.collect::<Result<Vec<_>,_>>().map_err(transport)?;
        let key=PrivateKeyDer::from_pem_file(&config.tls_key_file).map_err(transport)?;
        builder.with_client_auth_cert(certs,key).map_err(transport)
    }
}

#[cfg(test)]
#[path = "nats_startup_tests.rs"]
mod startup_tests;
