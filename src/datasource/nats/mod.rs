//! Native Core NATS and JetStream source. `Done` retires local request state;
//! only an explicit call on `ConsumerMessage::jetstream` acknowledges delivery.
mod source_metrics;
use source_metrics::SourceMetrics;
pub use async_nats as native;
use std::{collections::HashMap, error::Error, future::Future, pin::Pin, sync::{Arc,Mutex,Weak}};
use async_trait::async_trait;
use futures::StreamExt;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tokio::{sync::{Mutex as AsyncMutex, RwLock, mpsc},task::JoinHandle};
use tokio_util::sync::CancellationToken;
use crate::{operators::InputStream,runtime::{common::{Consumer,MessageContext,Payload,RuntimeEndpointConsumer,new_stream_id},config::{NatsDataConnectorConfig,NatsEndpointConfig,RuntimeEndpointConfig},datasource::{DataSource,DataSourceEndpointMetrics,StreamContext},environment::{Lifecycle,RuntimeEnvironment,RuntimeError,RuntimeResult},nats::{Connection,bounded,transport,CAPACITY,MAX_MESSAGE_BYTES}}};
pub type HandlerError=Box<dyn Error+Send+Sync>;
pub type HandlerResult<T=()>=Result<T,HandlerError>;
pub type BoxFuture<T>=Pin<Box<dyn Future<Output=T>+Send>>;
pub const IMPLEMENTATION:&str="rust/async-nats";
#[derive(Clone)]
pub struct ConsumerMessage { pub message:native::Message, pub jetstream:Option<native::jetstream::Message> }
pub type ResultCallback<H,T,R,E>=Arc<dyn Fn(MessageContext,StreamContext<T,R,E>,Arc<AsyncMutex<H>>,Payload<R>)->BoxFuture<bool>+Send+Sync>;
pub struct ResultContext<H:Send+'static,T:Send+Sync+'static,R:Send+Sync+'static,E:Send+Sync+'static>{
    callbacks:Mutex<HashMap<String,ResultCallback<H,T,R,E>>>,done:CancellationToken,
}
impl<H:Send+'static,T:Send+Sync+'static,R:Send+Sync+'static,E:Send+Sync+'static> ResultContext<H,T,R,E>{
    pub fn done(&self){self.done.cancel();}
    pub fn set_result_callback(&self,id:impl Into<String>,callback:ResultCallback<H,T,R,E>)->HandlerResult{
        let mut callbacks=self.callbacks.lock().unwrap();
        if self.done.is_cancelled() {return Err("NATS request already complete".into());}
        let id=id.into();
        if callbacks.contains_key(&id) || callbacks.len()>=CAPACITY {return Err("NATS duplicate result ID or callback capacity exhausted".into());}
        callbacks.insert(id,callback); Ok(())
    }
}
#[async_trait]
pub trait EndpointHandler<H,T,R,E>:Send+Sync where H:Send+'static,T:Send+Sync+'static,R:Send+Sync+'static,E:Send+Sync+'static {
    async fn on_error(&self,context:MessageContext,stream:StreamContext<T,R,E>,error:HandlerError);
    async fn begin_request(&self,context:MessageContext,stream:StreamContext<T,R,E>)->HandlerResult<(MessageContext,H)>;
    async fn consume_message(&self,context:MessageContext,stream:StreamContext<T,R,E>,state:Arc<AsyncMutex<H>>,message:ConsumerMessage,result:Arc<ResultContext<H,T,R,E>>)->HandlerResult;
    async fn get_message_id(&self,context:&MessageContext,stream:&StreamContext<T,R,E>,state:Arc<AsyncMutex<H>>,value:&R)->String;
    async fn end_request(&self,context:MessageContext,stream:StreamContext<T,R,E>,result:&HandlerResult,state:Arc<AsyncMutex<H>>);
}
struct Pending<H:Send+'static,T:Send+Sync+'static,R:Send+Sync+'static,E:Send+Sync+'static>{id:String,state:Arc<AsyncMutex<H>>,result:Arc<ResultContext<H,T,R,E>>,gate:RwLock<()>,context:MessageContext}
#[async_trait]
trait Endpoint:Send+Sync {async fn start(self:Arc<Self>,config:NatsDataConnectorConfig,context:MessageContext)->RuntimeResult<()>;async fn stop(&self,context:MessageContext)->RuntimeResult<()>;}
struct Running {stop:CancellationToken,context:MessageContext,task:JoinHandle<()>,error_task:JoinHandle<()>,connection:Arc<Connection>}
pub struct NatsDataSource {environment:RuntimeEnvironment,config:NatsDataConnectorConfig,endpoints:Mutex<Vec<Arc<dyn Endpoint>>>,started:AsyncMutex<bool>}
impl NatsDataSource{
    pub fn from_config(environment:RuntimeEnvironment,config:&NatsDataConnectorConfig)->RuntimeResult<Arc<Self>> {config.validate()?;Ok(Arc::new(Self{environment,config:config.clone(),endpoints:Mutex::new(vec![]),started:AsyncMutex::new(false)}))}
    pub fn add_endpoint<H,T,R,E,F>(&self,input:InputStream<T,R,E>,handler:Arc<F>)->RuntimeResult<Arc<NatsEndpoint<H,T,R,E,F>>> where H:Send+'static,T:Send+Sync+'static,R:Send+Sync+'static,E:Send+Sync+'static,F:EndpointHandler<H,T,R,E>+'static {
        let raw=self.environment.runtime_config().endpoint_by_id(input.endpoint_id()).ok_or_else(||RuntimeError::InvalidConfiguration("missing NATS endpoint".into()))?;
        let RuntimeEndpointConfig::Nats(config)=raw.as_ref() else {return Err(RuntimeError::InvalidConfiguration("expected NATS endpoint".into()));};
        config.validate(&self.config,true)?;
        let endpoint=Arc::new(NatsEndpoint{config:config.clone(),stream:StreamContext::new(input.clone()),handler,pending:Mutex::new(None),running:AsyncMutex::new(None),observations:SourceMetrics::new(self.environment.metrics(),self.config.jetstream(),&self.config.name,&config.name)?,metrics:DataSourceEndpointMetrics::from_input(&input)?});
        if input.result_stream().is_some(){input.set_result_consumer(Arc::new(Results{endpoint:Arc::downgrade(&endpoint)}));}
        self.environment.register_endpoint_consumer(endpoint.clone())?;
        self.endpoints.lock().unwrap().push(endpoint.clone());Ok(endpoint)
    }
}
impl DataSource for NatsDataSource {fn id(&self)->i32{self.config.id} fn name(&self)->&str{&self.config.name}}
#[async_trait]
impl Lifecycle for NatsDataSource{
    async fn start(&self,context:MessageContext)->RuntimeResult<()>{
        let mut started=self.started.lock().await;if *started{return Err(RuntimeError::ResourceAlreadyStarted(self.config.name.clone()));}
        let endpoints=self.endpoints.lock().unwrap().clone();let mut active:Vec<Arc<dyn Endpoint>>=vec![];
        for endpoint in endpoints {if let Err(e)=endpoint.clone().start(self.config.clone(),context.child()).await{for e in active{let _=e.stop(MessageContext::with_timeout(self.config.operation_timeout())).await;}return Err(e);}active.push(endpoint);}
        *started=true;Ok(())
    }
    async fn stop(&self,context:MessageContext)->RuntimeResult<()>{
        let mut started=self.started.lock().await;let endpoints=self.endpoints.lock().unwrap().clone();let mut result=Ok(());
        for endpoint in endpoints {if let Err(e)=endpoint.stop(context.clone()).await{result=Err(e);}}*started=false;result
    }
}
pub struct NatsEndpoint<H:Send+'static,T:Send+Sync+'static,R:Send+Sync+'static,E:Send+Sync+'static,F:EndpointHandler<H,T,R,E>+'static>{config:NatsEndpointConfig,stream:StreamContext<T,R,E>,handler:Arc<F>,pending:Mutex<Option<Arc<Pending<H,T,R,E>>>>,running:AsyncMutex<Option<Running>>,metrics:DataSourceEndpointMetrics,observations:SourceMetrics}
impl<H:Send+'static,T:Send+Sync+'static,R:Send+Sync+'static,E:Send+Sync+'static,F:EndpointHandler<H,T,R,E>+'static> RuntimeEndpointConsumer for NatsEndpoint<H,T,R,E,F>{fn id(&self)->i32{self.config.id}fn function_implementation(&self)->&'static str{std::any::type_name::<F>()}}
impl<H:Send+'static,T:Send+Sync+'static,R:Send+Sync+'static,E:Send+Sync+'static,F:EndpointHandler<H,T,R,E>+'static> NatsEndpoint<H,T,R,E,F>{
    async fn request(&self,owner:&MessageContext,message:ConsumerMessage){
        if message.message.payload.len()>MAX_MESSAGE_BYTES{self.handler.on_error(owner.clone(),self.stream.clone(),"NATS payload exceeds source buffer limit".into()).await;return;}
        self.observations.observe(&message);
        let mut metadata=owner.metadata().clone();
        if let Some(headers)=&message.message.headers{for (key,values) in headers.iter(){if let Some(value)=values.first(){metadata.insert(key.to_string(),value.to_string());}}}
        let tracing=self.stream.stream().environment().tracing_enabled();
        let mut context=if tracing{owner.child().with_metadata(metadata)}else{owner.child().with_metadata_untraced(metadata)};
        context=crate::runtime::datasource::apply_endpoint_tracing(context,self.stream.stream().environment(),self.config.id);
        let span=if tracing&&context.sampling_enabled(){let span=tracing::info_span!("nats.input",endpoint=self.config.name,subject=self.config.subject);let _=span.set_parent(context.open_telemetry_context().clone());context=context.with_span_context(&span);Some(span)}else{None};
        let _span=span;
        if context.stream_id().is_none(){context=context.with_stream_id(new_stream_id());}
        let started=self.metrics.request_start();
        let begin=tokio::select!{biased;_=owner.cancelled()=>Err(Box::new(RuntimeError::ContextCancelled) as HandlerError),value=self.handler.begin_request(context,self.stream.clone())=>value};
        let (context,state)=match begin {Ok(v)=>v,Err(e)=>{self.metrics.request_end(started,false);self.handler.on_error(owner.clone(),self.stream.clone(),e).await;return;}};
        let state=Arc::new(AsyncMutex::new(state));let result=Arc::new(ResultContext{callbacks:Mutex::new(HashMap::new()),done:CancellationToken::new()});
        let pending=Arc::new(Pending{id:context.stream_id().unwrap_or("").to_string(),state:state.clone(),result:result.clone(),gate:RwLock::new(()),context:context.clone()});
        *self.pending.lock().unwrap()=Some(pending.clone());self.metrics.pending_add(&pending.id);
        let outcome=tokio::select!{biased;_=owner.cancelled()=>Err(Box::new(RuntimeError::ContextCancelled) as HandlerError),_=context.cancelled()=>Err(Box::new(RuntimeError::ContextCancelled) as HandlerError),r=self.handler.consume_message(context.clone(),self.stream.clone(),state.clone(),message,result.clone())=>r};
        let outcome=if outcome.is_ok(){tokio::select!{biased;_=owner.cancelled()=>Err(Box::new(RuntimeError::ContextCancelled) as HandlerError),_=context.cancelled()=>Err(Box::new(RuntimeError::ContextCancelled) as HandlerError),_=result.done.cancelled()=>Ok(())}}else{outcome};
        *self.pending.lock().unwrap()=None;
        let _retire=pending.gate.write().await;
        result.done();result.callbacks.lock().unwrap().clear();self.metrics.pending_remove(&pending.id);
        self.handler.end_request(context,self.stream.clone(),&outcome,state).await;
        self.metrics.request_end(started,outcome.is_ok());
    }
}
#[async_trait]
impl<H:Send+'static,T:Send+Sync+'static,R:Send+Sync+'static,E:Send+Sync+'static,F:EndpointHandler<H,T,R,E>+'static> Endpoint for NatsEndpoint<H,T,R,E,F>{
    async fn start(self:Arc<Self>,config:NatsDataConnectorConfig,context:MessageContext)->RuntimeResult<()>{
        let mut running=self.running.lock().await;if running.is_some(){return Err(RuntimeError::ResourceAlreadyStarted(self.config.name.clone()));}if !self.config.enabled{return Ok(());}
        let (sender,mut errors)=mpsc::channel(CAPACITY);let connection=Connection::connect(&config,&context,sender).await?;
        let mut messages:Pin<Box<dyn futures::Stream<Item=RuntimeResult<ConsumerMessage>>+Send>>=bounded(&context,config.operation_timeout(),async {
            if config.jetstream(){let consumer=connection.consumer(&self.config).await?;let stream=consumer.stream().max_messages_per_batch(1).max_bytes_per_batch(MAX_MESSAGE_BYTES).messages().await.map_err(transport)?;
                Ok(Box::pin(stream.map(|r|r.map(|m|ConsumerMessage{message:m.message.clone(),jetstream:Some(m)}).map_err(transport))) as Pin<Box<dyn futures::Stream<Item=RuntimeResult<ConsumerMessage>>+Send>>)
            }else{let sub=if self.config.queue_group.is_empty(){connection.client.subscribe(self.config.subject.clone()).await}else{connection.client.queue_subscribe(self.config.subject.clone(),self.config.queue_group.clone()).await}.map_err(transport)?;
                connection.client.flush().await.map_err(transport)?;
                Ok(Box::pin(sub.map(|m|Ok(ConsumerMessage{message:m,jetstream:None}))) as Pin<Box<dyn futures::Stream<Item=RuntimeResult<ConsumerMessage>>+Send>>)}
        }).await?;
        let stop=CancellationToken::new();let task_stop=stop.clone();let owner=context.child();let task_context=owner.clone();let endpoint=self.clone();
        let error_endpoint=self.clone();let error_context=owner.clone();let error_stop=stop.clone();
        let error_task=tokio::spawn(async move{loop{tokio::select!{biased;_=error_stop.cancelled()=>break,_=error_context.cancelled()=>break,error=errors.recv()=>{let Some(error)=error else{break};tokio::select!{_=error_stop.cancelled()=>break,_=error_context.cancelled()=>break,_=error_endpoint.handler.on_error(error_context.clone(),error_endpoint.stream.clone(),Box::new(error))=>{}}}}}});
        let task=tokio::spawn(async move{loop{tokio::select!{biased;
            _=task_stop.cancelled()=>break,_=task_context.cancelled()=>break,
            message=messages.next()=>match message{Some(Ok(message))=>endpoint.request(&task_context,message).await,Some(Err(error))=>endpoint.handler.on_error(task_context.clone(),endpoint.stream.clone(),Box::new(error)).await,None=>{endpoint.handler.on_error(task_context.clone(),endpoint.stream.clone(),"NATS subscription closed".into()).await;break;}}
        }}});
        *running=Some(Running{stop,context:owner,task,error_task,connection});Ok(())
    }
    async fn stop(&self,context:MessageContext)->RuntimeResult<()>{
        let Some(mut running)=self.running.lock().await.take() else{return Ok(())};running.stop.cancel();running.error_task.abort();let _=running.error_task.await;
        let limit=context.clone().with_timeout_limit(std::time::Duration::from_secs(30));
        let drained=tokio::select!{_=limit.cancelled()=>false,r=&mut running.task=>{r.map_err(transport)?;true}};
        if !drained{running.context.cancel();running.task.abort();let _=running.task.await;*self.pending.lock().unwrap()=None;return Err(RuntimeError::ContextCancelled);}
        running.connection.drain(&limit,std::time::Duration::from_secs(5)).await
    }
}
struct Results<H:Send+'static,T:Send+Sync+'static,R:Send+Sync+'static,E:Send+Sync+'static,F:EndpointHandler<H,T,R,E>+'static>{endpoint:Weak<NatsEndpoint<H,T,R,E,F>>}
impl<H:Send+'static,T:Send+Sync+'static,R:Send+Sync+'static,E:Send+Sync+'static,F:EndpointHandler<H,T,R,E>+'static> Consumer<R> for Results<H,T,R,E,F>{
    async fn consume(&self,context:MessageContext,value:Payload<R>){
        let Some(endpoint)=self.endpoint.upgrade() else{return};let pending=endpoint.pending.lock().unwrap().clone();let Some(pending)=pending else{return};
        let _gate=pending.gate.read().await;if pending.result.done.is_cancelled()||context.stream_id()!=Some(pending.id.as_str()){return;}
        let id=endpoint.handler.get_message_id(&context,&endpoint.stream,pending.state.clone(),&value).await;
        let callback=pending.result.callbacks.lock().unwrap().remove(&id);
        if let Some(callback)=callback{let keep=tokio::select!{_=pending.context.cancelled()=>false,keep=callback(context,endpoint.stream.clone(),pending.state.clone(),value)=>keep};if keep&&!pending.result.done.is_cancelled(){pending.result.callbacks.lock().unwrap().insert(id,callback);}}
    }
}
