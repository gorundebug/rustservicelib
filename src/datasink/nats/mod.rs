//! Native asynchronous publish. Core acceptance, Core flush and JetStream
//! PubAck are distinct outcomes; none means downstream application completion.
pub use async_nats as native;
mod publication_metrics;
mod request_metrics;
use request_metrics::RequestMetrics;
use publication_metrics::PublicationMetrics;
use std::{error::Error,future::Future,pin::Pin,sync::{Arc,Mutex,Weak},marker::PhantomData};
use async_trait::async_trait;
use tokio::sync::{Mutex as AsyncMutex,Semaphore,mpsc};
use tokio_util::task::TaskTracker;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use crate::{operators::SinkStreamWithResult,runtime::{common::{Consumer,MessageContext,Payload,RuntimeEndpointConsumer,RuntimeStream},config::{NatsDataConnectorConfig,NatsEndpointConfig,RuntimeEndpointConfig},datasink::{DataSink,SinkStreamContext},environment::{Lifecycle,RuntimeEnvironment,RuntimeError,RuntimeResult},nats::{Connection,bounded,transport,CAPACITY,MAX_MESSAGE_BYTES,MAX_PENDING_BYTES}}};
pub type HandlerError=Box<dyn Error+Send+Sync>;
pub type HandlerResult<T=()>=Result<T,HandlerError>;
pub type BoxFuture<T>=Pin<Box<dyn Future<Output=T>+Send>>;
pub type StreamContext<T,R,E>=SinkStreamContext<T,R,E>;
pub const IMPLEMENTATION:&str="rust/async-nats";
#[derive(Debug)]
pub struct Delivery {pub flushed:bool,pub pub_ack:Option<native::jetstream::publish::PublishAck>}
type ErrorCallback=Arc<dyn Fn(MessageContext,HandlerError)->BoxFuture<()>+Send+Sync>;
struct Session{connection:Arc<Connection>,context:MessageContext,tasks:TaskTracker,count:Arc<Semaphore>,bytes:Arc<Semaphore>}
pub struct NatsDataSink{config:NatsDataConnectorConfig,environment:RuntimeEnvironment,session:Mutex<Option<Arc<Session>>>,lifecycle:AsyncMutex<()>,endpoints:Mutex<Vec<(NatsEndpointConfig,ErrorCallback)>>,errors:AsyncMutex<Option<tokio::task::JoinHandle<()>>>}
impl NatsDataSink{
    pub fn from_config(environment:RuntimeEnvironment,config:&NatsDataConnectorConfig)->RuntimeResult<Arc<Self>>{config.validate()?;Ok(Arc::new(Self{config:config.clone(),environment,session:Mutex::new(None),lifecycle:AsyncMutex::new(()),endpoints:Mutex::new(vec![]),errors:AsyncMutex::new(None)}))}
}
impl DataSink for NatsDataSink{fn id(&self)->i32{self.config.id}fn name(&self)->&str{&self.config.name}}
#[async_trait]
impl Lifecycle for NatsDataSink{
    async fn start(&self,context:MessageContext)->RuntimeResult<()>{
        let _guard=self.lifecycle.lock().await;if self.session.lock().unwrap().is_some(){return Err(RuntimeError::ResourceAlreadyStarted(self.config.name.clone()));}
        let endpoints=self.endpoints.lock().unwrap().clone();let (tx,mut rx)=mpsc::channel(CAPACITY);
        let connection=Connection::connect(&self.config,&context,tx).await?;
        if self.config.jetstream(){for (e,_) in &endpoints{if e.enabled{bounded(&context,self.config.operation_timeout(),async{connection.stream(e).await.map(|_|())}).await?;}}}
        let owner=context.child();let error_context=owner.clone();
        *self.errors.lock().await=Some(tokio::spawn(async move {loop{tokio::select!{biased;_=error_context.cancelled()=>break,e=rx.recv()=>{let Some(e)=e else{break};let text=e.to_string();for (_,callback) in &endpoints{tokio::select!{_=error_context.cancelled()=>break,_=callback(error_context.clone(),text.clone().into())=>{}}}}}}}));
        *self.session.lock().unwrap()=Some(Arc::new(Session{connection,context:owner,tasks:TaskTracker::new(),count:Arc::new(Semaphore::new(CAPACITY)),bytes:Arc::new(Semaphore::new(MAX_PENDING_BYTES))}));Ok(())
    }
    async fn stop(&self,context:MessageContext)->RuntimeResult<()>{
        let _guard=self.lifecycle.lock().await;let session=self.session.lock().unwrap().take();let Some(session)=session else{return Ok(())};
        session.count.close();session.bytes.close();session.tasks.close();let limit=context.with_timeout_limit(std::time::Duration::from_secs(30));
        let result=tokio::select!{_=limit.cancelled()=>Err(RuntimeError::ContextCancelled),_=session.tasks.wait()=>session.connection.drain(&limit,self.config.operation_timeout()).await};
        session.context.cancel();if let Some(task)=self.errors.lock().await.take(){task.abort();let _=task.await;}result
    }
}
struct Publication{completion:BoxFuture<HandlerResult<Delivery>>,_count:tokio::sync::OwnedSemaphorePermit,_bytes:tokio::sync::OwnedSemaphorePermit}
pub struct SinkMessage<R:Send+Sync+'static,E:Send+Sync+'static>{pub subject:String,pub payload:Vec<u8>,pub headers:native::HeaderMap,endpoint:NatsEndpointConfig,session:Arc<Session>,config:NatsDataConnectorConfig,context:MessageContext,tracing_enabled:bool,publication_metrics:Arc<PublicationMetrics>,collect:Arc<dyn Fn(MessageContext,R)->BoxFuture<()>+Send+Sync>,on_error:ErrorCallback,_error:PhantomData<E>}
impl<R:Send+Sync+'static,E:Send+Sync+'static> SinkMessage<R,E>{
    async fn publication(&self,flush:bool)->HandlerResult<Publication>{
        // Try-acquire makes overflow explicit and never retains a waiting payload.
        let size=self.payload.len()+self.headers.iter().map(|(k,values)|AsRef::<str>::as_ref(k).len()+values.iter().map(|v|v.as_str().len()).sum::<usize>()).sum::<usize>()+self.context.metadata().iter().map(|(k,v)|k.len()+v.len()).sum::<usize>();
        if size>MAX_MESSAGE_BYTES{return Err("NATS message exceeds 1 MiB limit".into());}
        if !crate::runtime::config::nats::valid_subject(&self.subject,false){return Err("invalid publish subject".into());}
        let count=self.session.count.clone().try_acquire_owned().map_err(|_|"NATS publisher capacity exhausted or stopped")?;
        let bytes=self.session.bytes.clone().try_acquire_many_owned(size.max(1) as u32).map_err(|_|"NATS publisher byte capacity exhausted or stopped")?;
        let mut headers=self.headers.clone();for (k,v) in self.context.transport_metadata_with_tracing(self.tracing_enabled){headers.insert(native::HeaderName::try_from(k)?,native::HeaderValue::try_from(v)?);}
        let session=self.session.clone();let context=self.context.clone().with_timeout_limit(self.config.operation_timeout());
        let payload=self.payload.clone().into();let subject=self.subject.clone();
        let ack=tokio::select!{biased;_=session.context.cancelled()=>return Err(Box::new(RuntimeError::ContextCancelled)),r=bounded(&context,self.config.operation_timeout(),async{
            if self.config.jetstream(){session.connection.js.send_publish(subject,native::jetstream::message::PublishMessage::build().payload(payload).headers(headers).expected_stream(&self.endpoint.stream_name)).await.map(Some).map_err(transport)}else{session.connection.client.publish_with_headers(subject,headers,payload).await.map(|_|None).map_err(transport)}
        })=>r?};
        let timeout=self.config.operation_timeout();
        Ok(Publication{_count:count,_bytes:bytes,completion:Box::pin(async move{tokio::select!{biased;_=session.context.cancelled()=>Err(Box::new(RuntimeError::ContextCancelled) as HandlerError),r=bounded(&context,timeout,async{if let Some(ack)=ack{Ok(Delivery{flushed:false,pub_ack:Some(ack.await.map_err(transport)?)})}else{if flush{session.connection.client.flush().await.map_err(transport)?;}Ok(Delivery{flushed:flush,pub_ack:None})}})=>r.map_err(|e|Box::new(e) as HandlerError)}})})
    }
    /// Admission uses native async publish; the acknowledgement/callback is
    /// tracked independently and included in Stop's bounded drain.
    pub async fn send<F>(&self,on_delivery:F)->HandlerResult where F:FnOnce(HandlerResult<Delivery>)->R+Send+'static{
        let mut publication=match self.publication(false).await {Ok(p)=>p,Err(e)=>{self.publication_metrics.error();return Err(e);}};let metrics=self.publication_metrics.clone();let collect=self.collect.clone();let context=self.context.clone();let owner=self.session.context.clone();let on_error=self.on_error.clone();
        self.session.tasks.spawn(async move{let outcome=publication.completion.as_mut().await;metrics.observe(&outcome);if let Err(e)=&outcome{tokio::select!{_=owner.cancelled()=>{},_=on_error(context.clone(),e.to_string().into())=>{}}}let value=on_delivery(outcome);tokio::select!{_=owner.cancelled()=>{},_=collect(context,value)=>{}}});Ok(())
    }
    /// Core waits for a server flush; JetStream waits for the native PubAck.
    pub async fn send_sync(&self)->HandlerResult<Delivery>{let outcome=async{let mut publication=self.publication(true).await?;publication.completion.as_mut().await}.await;self.publication_metrics.observe(&outcome);outcome}
    pub async fn out(&self,value:R){(self.collect)(self.context.clone(),value).await;}
    pub async fn skip(&self,value:R){self.out(value).await;}
}
#[async_trait]
pub trait EndpointHandler<H,T,R,E>:Send+Sync where H:Send+'static,T:Send+Sync+'static,R:Send+Sync+'static,E:Send+Sync+'static{
    fn get_stream_id(&self,context:&MessageContext,value:&T)->String;
    async fn on_error(&self,context:MessageContext,stream:StreamContext<T,R,E>,error:HandlerError);
    async fn begin_request(&self,context:MessageContext,stream:StreamContext<T,R,E>)->(MessageContext,H);
    async fn consume_message(&self,context:MessageContext,stream:StreamContext<T,R,E>,state:&mut H,value:Payload<T>,message:&mut SinkMessage<R,E>)->HandlerResult;
    async fn end_request(&self,context:MessageContext,stream:StreamContext<T,R,E>,result:&HandlerResult,state:H);
}
pub struct NatsEndpoint<H:Send+'static,T:Send+Sync+'static,R:Send+Sync+'static,E:Send+Sync+'static,F:EndpointHandler<H,T,R,E>+'static>{config:NatsEndpointConfig,sink:Arc<NatsDataSink>,stream:Weak<SinkStreamWithResult<T,R,E>>,handler:Arc<F>,publication_metrics:Arc<PublicationMetrics>,request_metrics:RequestMetrics,_state:PhantomData<fn()->H>}
pub fn make_nats_endpoint_consumer<H,T,R,E,F>(stream:&Arc<SinkStreamWithResult<T,R,E>>,sink:Arc<NatsDataSink>,handler:Arc<F>)->RuntimeResult<Arc<NatsEndpoint<H,T,R,E,F>>> where H:Send+'static,T:Send+Sync+'static,R:Send+Sync+'static,E:Send+Sync+'static,F:EndpointHandler<H,T,R,E>+'static{
    let raw=sink.environment.runtime_config().endpoint_by_id(stream.endpoint_id()).ok_or_else(||RuntimeError::InvalidConfiguration("missing NATS sink endpoint".into()))?;
    let RuntimeEndpointConfig::Nats(config)=raw.as_ref() else{return Err(RuntimeError::InvalidConfiguration("expected NATS endpoint".into()));};config.validate(&sink.config,false)?;
    let sc=SinkStreamContext::new(Arc::downgrade(stream));let error_handler=handler.clone();
    sink.endpoints.lock().unwrap().push((config.clone(),Arc::new(move|ctx,error|{let sc=sc.clone();let h=error_handler.clone();Box::pin(async move{h.on_error(ctx,sc,error).await})})));
    let publication_metrics=Arc::new(PublicationMetrics::new(sink.environment.metrics(),if sink.config.jetstream(){"jetstream"}else{"nats"},&sink.config.name,&config.name)?);
    let request_metrics=RequestMetrics::new(sink.environment.metrics(),if sink.config.jetstream(){"jetstream"}else{"nats"},&sink.config.name,&config.name)?;
    let endpoint=Arc::new(NatsEndpoint{publication_metrics,request_metrics,config:config.clone(),sink,stream:Arc::downgrade(stream),handler,_state:PhantomData});
    stream.set_sink_consumer(endpoint.clone())?;stream.environment().register_endpoint_consumer(endpoint.clone())?;Ok(endpoint)
}
impl<H:Send+'static,T:Send+Sync+'static,R:Send+Sync+'static,E:Send+Sync+'static,F:EndpointHandler<H,T,R,E>+'static> RuntimeEndpointConsumer for NatsEndpoint<H,T,R,E,F>{fn id(&self)->i32{self.config.id}fn function_implementation(&self)->&'static str{std::any::type_name::<F>()}}
impl<H:Send+'static,T:Send+Sync+'static,R:Send+Sync+'static,E:Send+Sync+'static,F:EndpointHandler<H,T,R,E>+'static> Consumer<T> for NatsEndpoint<H,T,R,E,F>{
    async fn consume(&self,context:MessageContext,value:Payload<T>){
        if !self.config.enabled{return;}let Some(stream)=self.stream.upgrade() else{return};let sc=SinkStreamContext::new(self.stream.clone());
        // Token and session are acquired under the same lock used by Stop.
        let admitted=self.sink.session.lock().unwrap().as_ref().map(|s|(s.clone(),s.tasks.token()));
        let Some((session,_token))=admitted else{self.handler.on_error(context,sc,"NATS sink is not running".into()).await;return;};
        let tracing_enabled=self.sink.environment.tracing_enabled();
        let mut context=context;
        let _span=if tracing_enabled&&context.sampling_enabled(){let span=tracing::info_span!("nats.output",endpoint=self.config.name,subject=self.config.subject);let _=span.set_parent(context.open_telemetry_context().clone());context=context.with_span_context(&span);Some(span)}else{None};
        let id=self.handler.get_stream_id(&context,&value);let context=context.with_stream_id(id);
        let request_metrics=self.request_metrics.start();
        let (context,mut state)=tokio::select!{biased;_=session.context.cancelled()=>return,_=context.cancelled()=>return,r=self.handler.begin_request(context.clone(),sc.clone())=>r};
        let collect_sc=sc.clone();let handler=self.handler.clone();let error_sc=sc.clone();
        let mut message=SinkMessage{subject:self.config.subject.clone(),payload:vec![],headers:native::HeaderMap::new(),endpoint:self.config.clone(),session:session.clone(),config:self.sink.config.clone(),context:context.clone(),tracing_enabled,publication_metrics:self.publication_metrics.clone(),collect:Arc::new(move|ctx,r|{let sc=collect_sc.clone();Box::pin(async move{sc.collect(ctx,r).await})}),on_error:Arc::new(move|ctx,e|{let handler=handler.clone();let sc=error_sc.clone();Box::pin(async move{handler.on_error(ctx,sc,e).await})}),_error:PhantomData};
        let result=tokio::select!{biased;_=session.context.cancelled()=>Err(Box::new(RuntimeError::ContextCancelled) as HandlerError),_=context.cancelled()=>Err(Box::new(RuntimeError::ContextCancelled) as HandlerError),r=self.handler.consume_message(context.clone(),sc.clone(),&mut state,value,&mut message)=>r};
        tokio::select!{_=session.context.cancelled()=>{},_=self.handler.end_request(context,sc,&result,state)=>{}};request_metrics.finish(result.is_ok());drop(stream);
    }
}
