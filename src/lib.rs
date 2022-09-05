use opentelemetry::{
    global,
    propagation::{Extractor, Injector},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};
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
pub struct PropagationContext {
    data: HashMap<String, String>,
}

impl PropagationContext {
    fn empty() -> Self {
        Self {
            data: HashMap::new(),
        }
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
        self.data.insert(key.to_owned(), value);
    }
}

impl Extractor for PropagationContext {
    fn get(&self, key: &str) -> Option<&str> {
        let key = key.to_owned();
        self.data.get(&key).map(|v| v.as_ref())
    }

    fn keys(&self) -> Vec<&str> {
        self.data.keys().map(|k| k.as_ref()).collect()
    }
}

pub fn setup_tracing(
    service_name: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    // Allows you to pass along context (i.e., trace IDs) across services
    global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());
    // Sets up the machinery needed to export data to Jaeger
    // There are other OTel crates that provide pipelines for the vendors
    // mentioned earlier.
    let tracer = opentelemetry_jaeger::new_pipeline()
        .with_service_name(service_name)
        .install_simple()?;

    // Create a tracing layer with the configured tracer
    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    // The SubscriberExt and SubscriberInitExt traits are needed to extend the
    // Registry to accept `opentelemetry (the OpenTelemetryLayer type).
    tracing_subscriber::registry()
        .with(opentelemetry)
        // Continue logging to stdout
        .with(fmt::Layer::default())
        .try_init()?;

    Ok(())
}
