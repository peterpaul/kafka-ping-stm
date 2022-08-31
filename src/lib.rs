use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartyId(pub Uuid);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
