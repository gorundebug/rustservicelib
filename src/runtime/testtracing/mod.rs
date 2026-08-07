use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    sync::{Arc, Mutex},
};

use tracing::{
    Event, Id, Subscriber,
    span::{Attributes, Record},
};
use tracing_subscriber::{Layer, registry::LookupSpan};

use crate::runtime::environment::{RuntimeResult, tracing::TracingEngine};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpanRecord {
    pub name: String,
    pub fields: BTreeMap<String, String>,
    pub events: Vec<EventRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventRecord {
    pub fields: BTreeMap<String, String>,
}

#[derive(Clone, Default)]
pub struct TestTracing {
    active: Arc<Mutex<HashMap<Id, SpanRecord>>>,
    finished: Arc<Mutex<Vec<SpanRecord>>>,
}

impl TestTracing {
    pub fn spans(&self) -> Vec<SpanRecord> {
        self.finished
            .lock()
            .expect("test tracing lock poisoned")
            .clone()
    }

    pub fn clear(&self) {
        self.active
            .lock()
            .expect("test tracing lock poisoned")
            .clear();
        self.finished
            .lock()
            .expect("test tracing lock poisoned")
            .clear();
    }
}

#[async_trait::async_trait]
impl TracingEngine for TestTracing {
    async fn shutdown(&self) -> RuntimeResult<()> {
        Ok(())
    }
}

#[derive(Default)]
struct FieldVisitor(BTreeMap<String, String>);

impl tracing::field::Visit for FieldVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}

impl<S> Layer<S> for TestTracing
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(
        &self,
        attributes: &Attributes<'_>,
        id: &Id,
        _context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut fields = FieldVisitor::default();
        attributes.record(&mut fields);
        self.active
            .lock()
            .expect("test tracing lock poisoned")
            .insert(
                id.clone(),
                SpanRecord {
                    name: attributes.metadata().name().to_owned(),
                    fields: fields.0,
                    events: Vec::new(),
                },
            );
    }

    fn on_record(
        &self,
        id: &Id,
        values: &Record<'_>,
        _context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut fields = FieldVisitor::default();
        values.record(&mut fields);
        if let Some(span) = self
            .active
            .lock()
            .expect("test tracing lock poisoned")
            .get_mut(id)
        {
            span.fields.extend(fields.0);
        }
    }

    fn on_event(&self, event: &Event<'_>, context: tracing_subscriber::layer::Context<'_, S>) {
        let Some(span) = context.event_span(event) else {
            return;
        };
        let mut fields = FieldVisitor::default();
        event.record(&mut fields);
        if let Some(record) = self
            .active
            .lock()
            .expect("test tracing lock poisoned")
            .get_mut(&span.id())
        {
            record.events.push(EventRecord { fields: fields.0 });
        }
    }

    fn on_close(&self, id: Id, _context: tracing_subscriber::layer::Context<'_, S>) {
        if let Some(span) = self
            .active
            .lock()
            .expect("test tracing lock poisoned")
            .remove(&id)
        {
            self.finished
                .lock()
                .expect("test tracing lock poisoned")
                .push(span);
        }
    }
}
