use serde::{Deserialize, Serialize};
use std::fmt::Debug;
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
