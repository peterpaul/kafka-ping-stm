use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Address {
    Broadcast,
    Direct(Uuid),
}

impl Address {
    fn is_for(&self, address: Uuid) -> bool {
        match self {
            Self::Broadcast => true,
            Self::Direct(uuid) => address == *uuid,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Envelope {
    pub sender: Address,
    pub receiver: Address,
    pub correlation_id: Uuid,
}

impl Envelope {
    fn new(sender: Address, receiver: Address, correlation_id: Uuid) -> Self {
        Self {
            sender,
            receiver,
            correlation_id,
        }
    }

    fn new_broadcast(sender: Uuid) -> Self {
        Self::new(Address::Direct(sender), Address::Broadcast, Uuid::new_v4())
    }

    fn respond_to(&self, sender: Uuid) -> Self {
        Self::new(Address::Direct(sender), self.sender, self.correlation_id)
    }

    pub fn is_directed_at(&self, address: Uuid) -> bool {
        self.receiver.is_for(address)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Ping {
    pub envelope: Envelope,
}

impl Ping {
    pub fn new(sender: Uuid) -> Self {
        Self {
            envelope: Envelope::new_broadcast(sender),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Pong {
    pub envelope: Envelope,
}

impl Pong {
    pub fn new(ping: &Ping, sender: Uuid) -> Self {
        Self {
            envelope: ping.envelope.respond_to(sender),
        }
    }
}
