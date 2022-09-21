use kafka::{
    consumer::Consumer,
    producer::{Producer, Record},
};
use opentelemetry::{
    global,
    propagation::{Extractor, Injector},
    Context,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{collections::HashMap, fmt::Debug};
use tokio::sync::{mpsc, oneshot};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartyId(pub Uuid);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Address {
    Broadcast,
    Multiple(Vec<PartyId>),
    Single(PartyId),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ping {
    session_id: Uuid,
}

impl Ping {
    pub fn new(session_id: Uuid) -> Self {
        Self { session_id }
    }

    pub fn session_id(&self) -> Uuid {
        self.session_id
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pong {
    session_id: Uuid,
}

impl Pong {
    pub fn new(session_id: Uuid) -> Self {
        Self { session_id }
    }

    pub fn session_id(&self) -> Uuid {
        self.session_id
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpannedMessage<T: Debug + Clone> {
    context: PropagationContext,
    body: T,
}

impl<T: Debug + Clone> SpannedMessage<T> {
    pub fn new(context: PropagationContext, body: T) -> Self {
        Self { context, body }
    }

    pub fn unwrap(self) -> T {
        self.body
    }

    pub fn context(&self) -> &PropagationContext {
        &self.context
    }
}

/// Serializable datastructure to hold the opentelemetry propagation context.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PropagationContext(HashMap<String, String>);

impl PropagationContext {
    fn empty() -> Self {
        Self(HashMap::new())
    }

    pub fn inject(context: &Context) -> Self {
        global::get_text_map_propagator(|propagator| {
            let mut propagation_context = PropagationContext::empty();
            propagator.inject_context(context, &mut propagation_context);
            propagation_context
        })
    }

    pub fn extract(&self) -> opentelemetry::Context {
        global::get_text_map_propagator(|propagator| propagator.extract(self))
    }
}

impl Injector for PropagationContext {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_owned(), value);
    }
}

impl Extractor for PropagationContext {
    fn get(&self, key: &str) -> Option<&str> {
        let key = key.to_owned();
        self.0.get(&key).map(|v| v.as_ref())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_ref()).collect()
    }
}

pub fn try_init_tracing(
    service_name: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    // Allows you to pass along context (i.e., trace IDs) across services
    global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());

    // Sets up the machinery needed to export data to Jaeger
    // There are other OTel crates that provide pipelines for the vendors
    // mentioned earlier.
    let tracer = opentelemetry_jaeger::new_pipeline()
        .with_service_name(service_name)
        // .install_batch(opentelemetry::runtime::Tokio)?;
        .install_simple()?;

    // Create a tracing layer with the configured tracer
    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    // Filter from the RUST_LOG environment variable
    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();

    // The SubscriberExt and SubscriberInitExt traits are needed to extend the
    // Registry to accept `opentelemetry (the OpenTelemetryLayer type).
    tracing_subscriber::registry()
        .with(opentelemetry)
        // Log to console filtered by level from RUST_LOG
        .with(fmt::Layer::default().with_filter(filter_layer))
        .try_init()
        .map_err(|e| e.into())
}

pub fn spawn_kafka_consumer_task<T>(
    mut consumer: Consumer,
) -> (
    tokio::task::JoinHandle<()>,
    mpsc::UnboundedReceiver<T>,
    oneshot::Sender<()>,
)
where
    T: DeserializeOwned + Debug + Send + Sync + 'static,
{
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
    let (message_tx, message_rx) = mpsc::unbounded_channel::<T>();

    let kafka_consumer_task = tokio::task::spawn_blocking(move || {
        while shutdown_rx.try_recv().is_err() {
            for msg_result in consumer.poll().unwrap().iter() {
                for msg in msg_result.messages() {
                    let message: T = serde_json::from_slice(msg.value).unwrap();
                    message_tx.send(message).unwrap();
                }
                consumer.consume_messageset(msg_result).unwrap();
            }
            consumer.commit_consumed().unwrap();
        }
    });

    (kafka_consumer_task, message_rx, shutdown_tx)
}

#[tracing::instrument(skip(producer), err)]
pub fn kafka_send_message<T>(
    producer: &mut Producer,
    queue: &str,
    message: T,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>
where
    T: serde::Serialize + Debug,
{
    log::debug!("Send message to `{}` queue: {:?}", queue, message);
    let json_message = serde_json::to_string_pretty(&message)?;
    let record = Record::from_value(queue, json_message);
    producer.send(&record).map_err(|e| e.into())
}

pub fn kafka_send_message_with_span<T>(
    producer: &mut Producer,
    queue: &str,
    message: T,
    span: tracing::Span,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>
where
    T: serde::Serialize + Debug + Clone,
{
    span.in_scope(|| {
        let propagation_context = PropagationContext::inject(&span.context());
        let spanned_message = SpannedMessage::new(propagation_context, message);
        kafka_send_message(producer, queue, spanned_message)
    })
}
