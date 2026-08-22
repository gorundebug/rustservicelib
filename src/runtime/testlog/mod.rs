use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
};

use tracing::{Event, Subscriber, field::Visit};
use tracing_subscriber::{Layer, registry::LookupSpan};

use crate::runtime::environment::{RuntimeResult, log::LogsEngine};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogRecord {
    pub level: tracing::Level,
    pub target: String,
    pub fields: BTreeMap<String, String>,
}

#[derive(Clone, Default)]
pub struct TestLog {
    records: Arc<Mutex<Vec<LogRecord>>>,
}

impl TestLog {
    pub fn records(&self) -> Vec<LogRecord> {
        self.records.lock().expect("test log lock poisoned").clone()
    }

    pub fn clear(&self) {
        self.records.lock().expect("test log lock poisoned").clear();
    }
}

#[async_trait::async_trait]
impl LogsEngine for TestLog {
    async fn shutdown(&self) -> RuntimeResult<()> {
        Ok(())
    }
}

#[derive(Default)]
struct FieldVisitor(BTreeMap<String, String>);

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}

impl<S> Layer<S> for TestLog
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_event(&self, event: &Event<'_>, _context: tracing_subscriber::layer::Context<'_, S>) {
        let mut fields = FieldVisitor::default();
        event.record(&mut fields);
        self.records
            .lock()
            .expect("test log lock poisoned")
            .push(LogRecord {
                level: *event.metadata().level(),
                target: event.metadata().target().to_owned(),
                fields: fields.0,
            });
    }
}
