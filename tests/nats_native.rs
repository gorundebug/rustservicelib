use std::{sync::Arc,time::Duration};
use async_trait::async_trait;
use futures::StreamExt;
use servicelib::{MessageContext,Payload,Consumer,Stream,api::DataConnectorType,runtime::{config::{NatsDataConnectorConfig,NatsEndpointConfig,RuntimeConfig,CallSemantics,InputStreamConfig,SinkStreamConfig,StreamConfig},environment::{RuntimeEnvironment,Lifecycle},datasource::StreamContext},datasource::nats::{self,ConsumerMessage,ResultContext,HandlerError,HandlerResult},datasink::nats as sink};
use tokio::sync::{mpsc,Mutex as AsyncMutex};
fn url()->String{std::env::var("NATS_URL").expect("NATS_URL is required: run this broker test explicitly")}
fn connector(js:bool)->NatsDataConnectorConfig{NatsDataConnectorConfig{id:1,name:"native".into(),kind:if js{DataConnectorType::JetStream}else{DataConnectorType::NATS},servers:vec![url()],operation_timeout_ms:2000,..Default::default()}}
fn endpoint(c:&NatsDataConnectorConfig,subject:&str)->NatsEndpointConfig{NatsEndpointConfig{id:2,name:"events".into(),id_data_connector:1,kind:c.kind,enabled:true,subject:subject.into(),..Default::default()}}
struct SourceHandler{received:mpsc::Sender<(ConsumerMessage,MessageContext)>,ends:mpsc::Sender<bool>,hold:bool,metrics:servicelib::runtime::environment::metrics::Metrics,protocol:&'static str}
#[async_trait]
impl nats::EndpointHandler<(),String,(),String> for SourceHandler{
 async fn on_error(&self,_:MessageContext,_:StreamContext<String,(),String>,error:HandlerError){eprintln!("source error: {error}");}
 async fn begin_request(&self,c:MessageContext,_:StreamContext<String,(),String>)->HandlerResult<(MessageContext,())>{
  let prefix=format!("broker_endpoint_source_observations_total{{connector=\"native\",endpoint=\"events\",observation=\"received\",protocol=\"{}\"}} ",self.protocol);
  assert!(self.metrics.render_prometheus().lines().any(|line| line.strip_prefix(&prefix).and_then(|n|n.parse::<u64>().ok()).is_some_and(|n|n>0)),"observation must precede BeginRequest");
  Ok((c,()))}
 async fn consume_message(&self,c:MessageContext,_:StreamContext<String,(),String>,_:Arc<AsyncMutex<()>>,m:ConsumerMessage,r:Arc<ResultContext<(),String,(),String>>)->HandlerResult{self.received.send((m,c)).await.unwrap();if !self.hold{r.done();}Ok(())}
 async fn get_message_id(&self,_:&MessageContext,_:&StreamContext<String,(),String>,_:Arc<AsyncMutex<()>>,_:&())->String{String::new()}
 async fn end_request(&self,_:MessageContext,_:StreamContext<String,(),String>,r:&HandlerResult,_:Arc<AsyncMutex<()>>){let _=self.ends.send(r.is_ok()).await;}
}
fn source(c:&NatsDataConnectorConfig,e:&NatsEndpointConfig,hold:bool)->(Arc<nats::NatsDataSource>,mpsc::Receiver<(ConsumerMessage,MessageContext)>,mpsc::Receiver<bool>){
 source_with_environment(c,e,hold,RuntimeEnvironment::default())
}
fn source_with_environment(c:&NatsDataConnectorConfig,e:&NatsEndpointConfig,hold:bool,env:RuntimeEnvironment)->(Arc<nats::NatsDataSource>,mpsc::Receiver<(ConsumerMessage,MessageContext)>,mpsc::Receiver<bool>){
 let metrics=env.metrics().clone();let input=InputStreamConfig{stream:StreamConfig::new(3,"input"),endpoint_id:2};
 env.publish_runtime_config(Arc::new(RuntimeConfig::from_parts(CallSemantics::FunctionCall,[],[input.clone().into()],[],[c.clone().into()],[e.clone().into()],[]).unwrap()));
 let input=servicelib::operators::InputStream::<String,(),String>::new(&input,env.clone());let source=nats::NatsDataSource::from_config(env,c).unwrap();
 let(tx,rx)=mpsc::channel(8);let(etx,erx)=mpsc::channel(8);source.add_endpoint(input,Arc::new(SourceHandler{received:tx,ends:etx,hold,metrics,protocol:if c.jetstream(){"jetstream"}else{"nats"}})).unwrap();(source,rx,erx)
}
async fn next<T>(rx:&mut mpsc::Receiver<T>)->T{tokio::time::timeout(Duration::from_secs(6),rx.recv()).await.unwrap().unwrap()}
#[tokio::test]
#[ignore="requires isolated real NATS broker"]
async fn done_does_not_ack_and_native_ack_survives_restart(){
 let c=connector(true);let unique=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();let mut e=endpoint(&c,&format!("rust.done.{unique}"));e.stream_name=format!("RUST_DONE_{unique}");e.durable_name="worker".into();e.create_stream=true;e.create_consumer=true;
 let client=async_nats::connect(url()).await.unwrap();let js=async_nats::jetstream::new(client);let _=js.delete_stream(&e.stream_name).await;
 let stream=js.create_stream(async_nats::jetstream::stream::Config{name:e.stream_name.clone(),subjects:vec![e.subject.clone()],..Default::default()}).await.unwrap();
 stream.create_consumer_strict(async_nats::jetstream::consumer::pull::Config{durable_name:Some(e.durable_name.clone()),filter_subject:e.subject.clone(),ack_wait:Duration::from_millis(250),ack_policy:async_nats::jetstream::consumer::AckPolicy::Explicit,..Default::default()}).await.unwrap();
 js.publish(e.subject.clone(),"payload".into()).await.unwrap().await.unwrap();let env=RuntimeEnvironment::default();let metrics=env.metrics().clone();let(s,mut rx,mut ends)=source_with_environment(&c,&e,false,env);s.start(MessageContext::new()).await.unwrap();
 let(first,_)=next(&mut rx).await;assert_eq!(first.message.payload.as_ref(),b"payload");assert!(next(&mut ends).await);
 let(second,_)=next(&mut rx).await;assert!(second.jetstream.as_ref().unwrap().info().unwrap().delivered>=2);second.jetstream.unwrap().double_ack().await.unwrap();assert!(next(&mut ends).await);
 assert_source_sample(&metrics,"jetstream","received",2);
 assert_source_sample(&metrics,"jetstream","redelivered",1);
 s.stop(MessageContext::with_timeout(Duration::from_secs(3))).await.unwrap();
 let(s,mut rx,_)=source(&c,&e,false);s.start(MessageContext::new()).await.unwrap();assert!(tokio::time::timeout(Duration::from_millis(600),rx.recv()).await.is_err());s.stop(MessageContext::with_timeout(Duration::from_secs(3))).await.unwrap();js.delete_stream(&e.stream_name).await.unwrap();
}
#[tokio::test]
#[ignore="requires isolated real NATS broker"]
async fn core_inherits_metadata_and_stop_is_bounded(){
 let c=connector(false);let unique=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();let e=endpoint(&c,&format!("rust.core.{unique}"));let env=RuntimeEnvironment::default();let metrics=env.metrics().clone();let(s,mut rx,_)=source_with_environment(&c,&e,true,env);
 let context=MessageContext::new().with_metadata([("owner".into(),"preserved".into())].into());s.start(context).await.unwrap();
 let client=async_nats::connect(url()).await.unwrap();client.publish(e.subject,"hello".into()).await.unwrap();client.flush().await.unwrap();let(m,ctx)=next(&mut rx).await;assert!(m.jetstream.is_none());assert_eq!(ctx.metadata().get("owner").unwrap(),"preserved");
 assert_source_sample(&metrics,"nats","received",1);
 assert!(!metrics.render_prometheus().contains("observation=\"redelivered\""));
 assert!(s.stop(MessageContext::with_timeout(Duration::from_millis(100))).await.is_err());assert!(ctx.is_cancelled());
}
struct SinkHandler{results:mpsc::Sender<Result<bool,String>>,async_send:bool}
#[async_trait]
impl sink::EndpointHandler<(),String,bool,String> for SinkHandler{
 fn get_stream_id(&self,c:&MessageContext,_:&String)->String{c.stream_id().unwrap_or("sink-request").into()}
 async fn on_error(&self,_:MessageContext,_:sink::StreamContext<String,bool,String>,e:sink::HandlerError){let _=self.results.send(Err(e.to_string())).await;}
 async fn begin_request(&self,c:MessageContext,_:sink::StreamContext<String,bool,String>)->(MessageContext,()){(c,())}
 async fn consume_message(&self,_:MessageContext,_:sink::StreamContext<String,bool,String>,_:&mut (),value:Payload<String>,message:&mut sink::SinkMessage<bool,String>)->sink::HandlerResult{
  message.payload=value.as_bytes().to_vec();if self.async_send{let tx=self.results.clone();message.send(move|r|{let ok=r.is_ok();let _=tx.try_send(r.map(|d|d.pub_ack.is_some()).map_err(|e|e.to_string()));ok}).await?;}else{let d=message.send_sync().await?;self.results.send(Ok(d.flushed)).await.unwrap();}Ok(())
 }
 async fn end_request(&self,_:MessageContext,_:sink::StreamContext<String,bool,String>,r:&sink::HandlerResult,_:()){if let Err(e)=r{let _=self.results.send(Err(e.to_string())).await;}}
}
fn make_sink(c:&NatsDataConnectorConfig,e:&NatsEndpointConfig,asynchronous:bool)->(Arc<sink::NatsDataSink>,Arc<impl Consumer<String> + use<>>,mpsc::Receiver<Result<bool,String>>){
 make_sink_with_environment(c,e,asynchronous,RuntimeEnvironment::default())
}
fn make_sink_with_environment(c:&NatsDataConnectorConfig,e:&NatsEndpointConfig,asynchronous:bool,env:RuntimeEnvironment)->(Arc<sink::NatsDataSink>,Arc<impl Consumer<String> + use<>>,mpsc::Receiver<Result<bool,String>>){
 let sc=SinkStreamConfig{stream:StreamConfig::new(4,"sink"),endpoint_id:2};let source=StreamConfig::new(3,"upstream");env.publish_runtime_config(Arc::new(RuntimeConfig::from_parts(CallSemantics::FunctionCall,[],[source.clone().into(),sc.clone().into()],[],[c.clone().into()],[e.clone().into()],[]).unwrap()));
 let stream=Stream::<String>::new(&source,env.clone()).sink_with_result::<bool,String>(&sc).unwrap();let sink=sink::NatsDataSink::from_config(env,c).unwrap();let(tx,rx)=mpsc::channel(8);let consumer=sink::make_nats_endpoint_consumer(&stream,sink.clone(),Arc::new(SinkHandler{results:tx,async_send:asynchronous})).unwrap();
 // Keep stream alive while returned consumer owns only a Weak stream reference.
 struct Owned<C>{consumer:Arc<C>,_stream:Arc<servicelib::operators::SinkStreamWithResult<String,bool,String>>}
 impl<C:Consumer<String>> Consumer<String> for Owned<C>{async fn consume(&self,c:MessageContext,p:Payload<String>){self.consumer.consume(c,p).await;}}
 (sink,Arc::new(Owned{consumer,_stream:stream}),rx)
}
#[tokio::test]
#[ignore="requires isolated real NATS broker"]
async fn core_flush_and_jetstream_async_puback_are_distinct(){
 for js_mode in [false,true]{let c=connector(js_mode);let mut e=endpoint(&c,if js_mode{"rust.publish.js"}else{"rust.publish.core"});if js_mode{e.stream_name="RUST_PUBLISH".into();e.create_stream=true;}
 let client=async_nats::connect(url()).await.unwrap();let mut subscriber=client.subscribe(e.subject.clone()).await.unwrap();client.flush().await.unwrap();let(s,consumer,mut results)=make_sink(&c,&e,js_mode);s.start(MessageContext::new()).await.unwrap();consumer.consume(MessageContext::new(),Payload::new("native".into())).await;assert!(next(&mut results).await.unwrap());assert_eq!(tokio::time::timeout(Duration::from_secs(3),subscriber.next()).await.unwrap().unwrap().payload.as_ref(),b"native");s.stop(MessageContext::with_timeout(Duration::from_secs(3))).await.unwrap();assert!(results.try_recv().is_err(),"graceful stop must not report a transport failure");if js_mode{async_nats::jetstream::new(client).delete_stream(e.stream_name).await.unwrap();}}
}
#[tokio::test]
#[ignore="requires isolated real NATS broker"]
async fn missing_resources_are_not_created_without_flags(){let c=connector(true);let mut e=endpoint(&c,"rust.missing");e.stream_name="RUST_MISSING".into();e.durable_name="worker".into();let(s,_,_)=source(&c,&e,false);assert!(s.start(MessageContext::new()).await.is_err());let js=async_nats::jetstream::new(async_nats::connect(url()).await.unwrap());assert!(js.get_stream("RUST_MISSING").await.is_err());}
#[tokio::test]
#[ignore="requires isolated real NATS broker"]
async fn oversized_publish_and_missing_stream_surface_errors(){
 let c=connector(false);let e=endpoint(&c,"rust.overflow");let(s,consumer,mut results)=make_sink(&c,&e,true);s.start(MessageContext::new()).await.unwrap();consumer.consume(MessageContext::new(),Payload::new("x".repeat(1024*1024+1))).await;assert!(next(&mut results).await.unwrap_err().contains("1 MiB"));s.stop(MessageContext::with_timeout(Duration::from_secs(3))).await.unwrap();
 let c=connector(true);let mut e=endpoint(&c,"rust.gone");e.stream_name="RUST_GONE".into();e.create_stream=true;let(s,consumer,mut results)=make_sink(&c,&e,true);s.start(MessageContext::new()).await.unwrap();let js=async_nats::jetstream::new(async_nats::connect(url()).await.unwrap());js.delete_stream(&e.stream_name).await.unwrap();consumer.consume(MessageContext::new(),Payload::new("delivery".into())).await;assert!(next(&mut results).await.is_err());s.stop(MessageContext::with_timeout(Duration::from_secs(3))).await.unwrap();
}
#[tokio::test]
#[ignore="requires isolated real NATS broker"]
async fn incompatible_existing_consumer_is_rejected_without_mutation(){
 let c=connector(true);let mut e=endpoint(&c,"rust.incompatible");e.stream_name="RUST_INCOMPATIBLE".into();e.durable_name="existing".into();e.create_stream=true;e.create_consumer=true;
 let js=async_nats::jetstream::new(async_nats::connect(url()).await.unwrap());let stream=js.create_stream(async_nats::jetstream::stream::Config{name:e.stream_name.clone(),subjects:vec![e.subject.clone()],..Default::default()}).await.unwrap();
 stream.create_consumer_strict(async_nats::jetstream::consumer::pull::Config{durable_name:Some(e.durable_name.clone()),filter_subject:e.subject.clone(),ack_policy:async_nats::jetstream::consumer::AckPolicy::None,..Default::default()}).await.unwrap();let(s,_,_)=source(&c,&e,false);assert!(s.start(MessageContext::new()).await.is_err());assert_eq!(stream.consumer_info(&e.durable_name).await.unwrap().config.ack_policy,async_nats::jetstream::consumer::AckPolicy::None);js.delete_stream(e.stream_name).await.unwrap();
}
#[tokio::test]
#[ignore="requires isolated real NATS broker"]
async fn native_reconnect_restores_core_subscription(){
 let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();let address=listener.local_addr().unwrap();let (disconnect,epoch)=tokio::sync::watch::channel(0u32);let upstream=url().trim_start_matches("nats://").to_string();
 let proxy=tokio::spawn(async move{let mut tasks=tokio::task::JoinSet::new();loop{tokio::select!{accepted=listener.accept()=>{let(mut downstream,_)=accepted.unwrap();let upstream=upstream.clone();let mut epoch=epoch.clone();epoch.borrow_and_update();tasks.spawn(async move{let mut broker=tokio::net::TcpStream::connect(upstream).await.unwrap();tokio::select!{_=epoch.changed()=>{},_=tokio::io::copy_bidirectional(&mut downstream,&mut broker)=>{}}});},_=tasks.join_next(),if !tasks.is_empty()=>{}}}});
 let mut c=connector(false);c.servers=vec![format!("nats://{address}")];let e=endpoint(&c,"rust.reconnect");let(s,mut received,mut ends)=source(&c,&e,false);s.start(MessageContext::new()).await.unwrap();let client=async_nats::connect(url()).await.unwrap();client.publish(e.subject.clone(),"before".into()).await.unwrap();client.flush().await.unwrap();assert_eq!(next(&mut received).await.0.message.payload.as_ref(),b"before");assert!(next(&mut ends).await);disconnect.send(1).unwrap();
 let message=tokio::time::timeout(Duration::from_secs(6),async{loop{client.publish(e.subject.clone(),"after".into()).await.unwrap();client.flush().await.unwrap();tokio::select!{m=received.recv()=>break m.unwrap(),_=tokio::time::sleep(Duration::from_millis(100))=>{}}}}).await.unwrap();assert_eq!(message.0.message.payload.as_ref(),b"after");assert!(next(&mut ends).await);s.stop(MessageContext::with_timeout(Duration::from_secs(3))).await.unwrap();proxy.abort();let _=proxy.await;
}

#[tokio::test]
#[ignore="requires isolated real NATS broker"]
async fn publication_metrics_native_boundaries() {
 for (js_mode, asynchronous, outcome) in [(false,true,"client-accepted"),(false,false,"flushed"),(true,true,"acknowledged"),(true,false,"acknowledged")] {
  let c=connector(js_mode);let mut e=endpoint(&c,"rust.metrics");
  if js_mode {e.stream_name="RUST_METRICS".into();e.create_stream=true;}
  let env=RuntimeEnvironment::default();let metrics=env.metrics().clone();
  let(s,consumer,mut results)=make_sink_with_environment(&c,&e,asynchronous,env);
  s.start(MessageContext::new()).await.unwrap();
  consumer.consume(MessageContext::new(),Payload::new("publication".into())).await;
  next(&mut results).await.unwrap();
  let protocol=if js_mode {"jetstream"}else{"nats"};
  let sample=format!("broker_endpoint_publication_outcomes_total{{connector=\"native\",endpoint=\"events\",outcome=\"{outcome}\",protocol=\"{protocol}\"}} 1");
  assert!(metrics.render_prometheus().contains(&sample),"{}",metrics.render_prometheus());
  consumer.consume(MessageContext::new(),Payload::new("x".repeat(1024*1024+1))).await;
  assert!(next(&mut results).await.is_err());
  let error=format!("broker_endpoint_publication_outcomes_total{{connector=\"native\",endpoint=\"events\",outcome=\"error\",protocol=\"{protocol}\"}} 1");
  assert!(metrics.render_prometheus().contains(&error));
  let rendered=metrics.render_prometheus();
  assert!(rendered.contains(&format!("datasink_endpoint_messages_total{{connector=\"native\",endpoint=\"events\",protocol=\"{protocol}\"}} 1")), "{rendered}");
  assert!(rendered.contains(&format!("datasink_endpoint_events_total{{connector=\"native\",endpoint=\"events\",event=\"request_error\",protocol=\"{protocol}\"}} 1")), "{rendered}");
  assert!(rendered.contains(&format!("datasink_endpoint_active_requests{{connector=\"native\",endpoint=\"events\",protocol=\"{protocol}\"}} 0")), "{rendered}");
  s.stop(MessageContext::with_timeout(Duration::from_secs(3))).await.unwrap();
  assert!(metrics.render_prometheus().contains(&sample));
  if js_mode {async_nats::jetstream::new(async_nats::connect(url()).await.unwrap()).delete_stream(&e.stream_name).await.unwrap();}
 }
}

fn assert_source_sample(metrics:&servicelib::runtime::environment::metrics::Metrics,protocol:&str,observation:&str,count:u64){
 let sample=format!("broker_endpoint_source_observations_total{{connector=\"native\",endpoint=\"events\",observation=\"{observation}\",protocol=\"{protocol}\"}} {count}");
 let text=metrics.render_prometheus();assert!(text.lines().any(|line|line==sample),"{text}");
}

// Raw transport acceptance: the graph need not serialize native bytes or headers as JSON.
struct RawSinkHandler;
#[async_trait]
impl sink::EndpointHandler<(), Vec<u8>, bool, String> for RawSinkHandler {
 fn get_stream_id(&self, c: &MessageContext, _: &Vec<u8>) -> String { c.stream_id().unwrap().into() }
 async fn on_error(&self, _: MessageContext, _: sink::StreamContext<Vec<u8>, bool, String>, e: sink::HandlerError) { panic!("raw publication failed: {e}"); }
 async fn begin_request(&self, c: MessageContext, _: sink::StreamContext<Vec<u8>, bool, String>) -> (MessageContext, ()) { (c, ()) }
 async fn consume_message(&self, _: MessageContext, _: sink::StreamContext<Vec<u8>, bool, String>, _: &mut (), value: Payload<Vec<u8>>, message: &mut sink::SinkMessage<bool, String>) -> sink::HandlerResult {
  message.payload = (*value).clone();
  message.headers.append("X-Repeated", "first");
  message.headers.append("X-Repeated", "second");
  message.send_sync().await?;
  Ok(())
 }
 async fn end_request(&self, _: MessageContext, _: sink::StreamContext<Vec<u8>, bool, String>, result: &sink::HandlerResult, _: ()) { assert!(result.is_ok(), "{result:?}"); }
}

#[tokio::test]
#[ignore = "requires isolated real NATS broker"]
async fn raw_bytes_repeated_headers_and_correlation_survive_concurrent_publication() {
 for js_mode in [false, true] {
  let unique = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
  let c = connector(js_mode);
  let mut e = endpoint(&c, &format!("rust.raw.{unique}"));
  if js_mode { e.stream_name = format!("RUST_RAW_{unique}"); e.durable_name = "worker".into(); e.create_stream = true; e.create_consumer = true; }
  let (source, mut received, mut ends) = source(&c, &e, false);
  source.start(MessageContext::new()).await.unwrap();
  // Sink connects to the stream created by the source, without consumer settings.
  e.durable_name.clear();
  e.create_consumer = false;
  let env = RuntimeEnvironment::default();
  let sc = SinkStreamConfig { stream: StreamConfig::new(4, "sink"), endpoint_id: 2 };
  let upstream = StreamConfig::new(3, "upstream");
  env.publish_runtime_config(Arc::new(RuntimeConfig::from_parts(CallSemantics::FunctionCall, [], [upstream.clone().into(), sc.clone().into()], [], [c.clone().into()], [e.clone().into()], []).unwrap()));
  let stream = Stream::<Vec<u8>>::new(&upstream, env.clone()).sink_with_result::<bool, String>(&sc).unwrap();
  let sink = sink::NatsDataSink::from_config(env, &c).unwrap();
  let consumer = sink::make_nats_endpoint_consumer(&stream, sink.clone(), Arc::new(RawSinkHandler)).unwrap();
  sink.start(MessageContext::new()).await.unwrap();
  let mut sends = tokio::task::JoinSet::new();
  for id in 0u8..8 {
   let consumer = consumer.clone();
   sends.spawn(async move { consumer.consume(MessageContext::new().with_stream_id(format!("raw-{id}")), Payload::new(vec![0, 255, 128, id, 0])).await; });
  }
  let mut seen = std::collections::HashSet::new();
  for _ in 0..8 {
   let (message, context) = next(&mut received).await;
   let bytes = message.message.payload.as_ref();
   assert_eq!(bytes.len(), 5);
   let id = bytes[3];
   assert!(id < 8 && seen.insert(id));
   assert_eq!(bytes, &[0, 255, 128, id, 0]);
   let headers = message.message.headers.as_ref().unwrap();
   assert_eq!(headers.get_all("X-Repeated").map(|v| v.as_str()).collect::<Vec<_>>(), vec!["first", "second"]);
   assert_eq!(context.stream_id(), Some(format!("raw-{id}").as_str()));
   assert_eq!(headers.get("x-stream-id").unwrap().as_str(), format!("raw-{id}"));
   if let Some(native) = message.jetstream { native.double_ack().await.unwrap(); }
   assert!(next(&mut ends).await);
  }
  while let Some(result) = tokio::time::timeout(Duration::from_secs(6), sends.join_next()).await.unwrap() { result.unwrap(); }
  sink.stop(MessageContext::with_timeout(Duration::from_secs(3))).await.unwrap();
  source.stop(MessageContext::with_timeout(Duration::from_secs(3))).await.unwrap();
  if js_mode { async_nats::jetstream::new(async_nats::connect(url()).await.unwrap()).delete_stream(&e.stream_name).await.unwrap(); }
 }
}
