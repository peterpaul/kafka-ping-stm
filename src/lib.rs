use kafka::consumer::Consumer;
use opentelemetry::{
    global,
    propagation::{Extractor, Injector},
    sdk::trace::Tracer,
    sdk::Resource,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{collections::HashMap, fmt::Debug};
use tokio::sync::{mpsc, oneshot};
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
pub struct Envelope<T: Debug + Clone> {
    source: PartyId,
    destination: Address,
    body: T,
}

impl<T: Debug + Clone> Envelope<T> {
    pub fn new(source: PartyId, destination: Address, body: T) -> Self {
        Self {
            source,
            destination,
            body,
        }
    }

    pub fn source(&self) -> PartyId {
        self.source.clone()
    }

    pub fn destination(&self) -> Address {
        self.destination.clone()
    }

    pub fn body(&self) -> T {
        self.body.clone()
    }

    pub fn is_directed_at(&self, address: Uuid) -> bool {
        let party_id = PartyId(address);
        match &self.destination {
            Address::Broadcast => true,
            Address::Multiple(addresses) => addresses.contains(&party_id),
            Address::Single(address) => *address == party_id,
        }
    }

    pub fn map_body<F, S>(self, f: F) -> Envelope<S>
    where
        F: FnOnce(T) -> S,
        S: Clone + Debug,
    {
        Envelope::new(self.source, self.destination, f(self.body))
    }
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

#[derive(Debug, Clone)]
pub struct Spanned<T> {
    span: tracing::Span,
    inner: T,
}

impl<T> Spanned<T> {
    pub fn new(span: tracing::Span, inner: T) -> Self {
        Self { span, inner }
    }

    pub fn new_cloned(span: &tracing::Span, inner: T) -> Self {
        Self {
            span: span.clone(),
            inner,
        }
    }

    pub fn unwrap(self) -> T {
        self.inner
    }

    pub fn span(&self) -> tracing::Span {
        self.span.clone()
    }
}

impl<'a, T> Spanned<T> {
    pub fn inner(&'a self) -> &'a T {
        &self.inner
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

    pub fn with_context<F, R>(&self, f: F) -> R
    where
        F: Fn(&PropagationContext) -> R,
    {
        f(&self.context)
    }
}

/// Serializable datastructure to hold the opentelemetry propagation context.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PropagationContext(HashMap<String, String>);

impl PropagationContext {
    fn empty() -> Self {
        Self(HashMap::new())
    }

    pub fn inject(context: &opentelemetry::Context) -> Self {
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

fn get_tracer_for_jaeger(
    service_name: &str,
) -> Result<Tracer, Box<dyn std::error::Error + Send + Sync + 'static>> {
    // Sets up the machinery needed to export data to Jaeger
    // There are other OTel crates that provide pipelines for the vendors
    // mentioned earlier.
    let tracer = opentelemetry_jaeger::new_pipeline()
        .with_service_name(service_name)
        // .install_batch(opentelemetry::runtime::Tokio)?;
        .install_simple()?;
    Ok(tracer)
}

fn _get_tracer_for_otlp() -> Result<Tracer, Box<dyn std::error::Error + Send + Sync + 'static>> {
    // use tonic as grpc layer here.
    // If you want to use grpcio. enable `grpc-sys` feature and use with_grpcio function here.
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(opentelemetry_otlp::new_exporter().tonic())
        .with_trace_config(opentelemetry::sdk::trace::config().with_resource(Resource::default()))
        .install_batch(opentelemetry::runtime::Tokio)?;
    // .install_simple()?;
    Ok(tracer)
}

pub fn setup_tracing_from_environment(
    service_name: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    if std::env::var("JAEGER_TRACING").is_ok() {
        println!("Initializing tracing with jaeger/opentelemetry");
        setup_tracing_with_jaeger(service_name)
    } else {
        println!("Initializing tracing to console only");
        println!("  - set environment variable 'JAEGER_TRACING' to enable jaeger");
        setup_tracing_without_jaeger()
    }
}

pub fn setup_tracing_without_jaeger(
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    // Allows you to pass along context (i.e., trace IDs) across services
    global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());

    // Filter from the RUST_LOG environment variable
    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();

    // The SubscriberExt and SubscriberInitExt traits are needed to extend the
    // Registry to accept `opentelemetry (the OpenTelemetryLayer type).
    tracing_subscriber::registry()
        // Log to console filtered by level from RUST_LOG
        .with(fmt::Layer::default())
        .with(filter_layer)
        .try_init()?;

    Ok(())
}

pub fn setup_tracing_with_jaeger(
    service_name: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    // Allows you to pass along context (i.e., trace IDs) across services
    global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());

    let tracer = get_tracer_for_jaeger(service_name)?;
    // let tracer = get_tracer_for_otlp()?;

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
        .try_init()?;

    Ok(())
}

fn poll_kafka<T>(
    mut consumer: Consumer,
    message_tx: mpsc::UnboundedSender<T>,
    mut shutdown_rx: oneshot::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>
where
    T: DeserializeOwned + Send + Sync + Debug + 'static,
{
    while shutdown_rx.try_recv().is_err() {
        log::trace!("polling kafka for messages");
        for msg_result in consumer.poll()?.iter() {
            log::trace!("polled messages: {}", msg_result.messages().len());
            for msg in msg_result.messages() {
                let message: T = serde_json::from_slice(msg.value)?;
                message_tx.send(message)?;
            }
            consumer.consume_messageset(msg_result)?;
        }
        consumer.commit_consumed()?;
    }
    Ok(())
}

pub fn spawn_kafka_consumer_task<T>(
    consumer: Consumer,
) -> (
    tokio::task::JoinHandle<()>,
    mpsc::UnboundedReceiver<T>,
    oneshot::Sender<()>,
)
where
    T: DeserializeOwned + Debug + Send + Sync + 'static,
{
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let (message_tx, message_rx) = mpsc::unbounded_channel::<T>();

    let kafka_consumer_task = tokio::task::spawn_blocking(move || {
        poll_kafka(consumer, message_tx, shutdown_rx).unwrap();
    });

    (kafka_consumer_task, message_rx, shutdown_tx)
}
