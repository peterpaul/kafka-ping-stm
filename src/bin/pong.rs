use kafka_ping_stm::{Address, Ping, Pong};

use kafka::consumer::{Consumer, FetchOffset};
use kafka::producer::{Producer, Record, RequiredAcks};
use oblivious_state_machine::{
    state::{DeliveryStatus, State, StateTypes, Transition},
    state_machine::TimeBoundStateMachineRunner,
};
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug)]
enum InboundMessage {
    PingReceived(Ping),
    PongSent(Pong),
}

#[derive(Debug)]
struct Types;
impl StateTypes for Types {
    type In = InboundMessage;
    type Out = Pong;
    type Err = String;
}

#[derive(Debug)]
struct ListeningForPing {
    my_address: Uuid,
    received_ping: Option<Ping>,
}

impl ListeningForPing {
    fn new(my_address: Uuid) -> Self {
        Self {
            my_address,
            received_ping: None,
        }
    }

    fn receive_ping(&mut self, ping: Ping) {
        log::info!("Received Ping: {:?}", ping);
        self.received_ping = Some(ping);
    }
}

impl State<Types> for ListeningForPing {
    fn desc(&self) -> String {
        "Waiting for Ping".to_owned()
    }

    fn deliver(
        &mut self,
        message: InboundMessage,
    ) -> DeliveryStatus<InboundMessage, <Types as StateTypes>::Err> {
        match message {
            InboundMessage::PingReceived(ping) => {
                self.receive_ping(ping);
                DeliveryStatus::Delivered
            }
            _ => DeliveryStatus::Unexpected(message),
        }
    }

    fn advance(&self) -> Result<Transition<Types>, <Types as StateTypes>::Err> {
        let next = match &self.received_ping {
            Some(ping) => {
                Transition::Next(Box::new(SendingPong::new(self.my_address, ping.clone())))
            }
            None => Transition::Same,
        };
        Ok(next)
    }
}

struct SendingPong {
    my_address: Uuid,
    received_ping: Ping,
    sent_pong: Option<Pong>,
}

impl SendingPong {
    fn new(my_address: Uuid, received_ping: Ping) -> Self {
        Self {
            my_address,
            received_ping,
            sent_pong: None,
        }
    }

    fn get_pong(&self) -> Pong {
        Pong::new(&self.received_ping, self.my_address)
    }
}

impl State<Types> for SendingPong {
    fn desc(&self) -> String {
        "Sending Pong".to_owned()
    }

    fn initialize(&self) -> Vec<Pong> {
        vec![self.get_pong()]
    }

    fn deliver(
        &mut self,
        message: InboundMessage,
    ) -> DeliveryStatus<InboundMessage, <Types as StateTypes>::Err> {
        match message {
            InboundMessage::PongSent(pong) => {
                self.sent_pong = Some(pong);
                DeliveryStatus::Delivered
            }
            _ => DeliveryStatus::Unexpected(message),
        }
    }

    fn advance(&self) -> Result<Transition<Types>, <Types as StateTypes>::Err> {
        let next = match &self.sent_pong {
            Some(_pong) => Transition::Next(Box::new(SentPong::new())),
            None => Transition::Same,
        };
        Ok(next)
    }
}

struct SentPong;

impl SentPong {
    fn new() -> Self {
        SentPong
    }
}

impl State<Types> for SentPong {
    fn desc(&self) -> String {
        "Sent Pong".to_owned()
    }

    fn deliver(
        &mut self,
        message: <Types as StateTypes>::In,
    ) -> DeliveryStatus<<Types as StateTypes>::In, <Types as StateTypes>::Err> {
        DeliveryStatus::Unexpected(message)
    }

    fn advance(&self) -> Result<Transition<Types>, <Types as StateTypes>::Err> {
        Ok(Transition::Terminal)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    pretty_env_logger::init();

    let address = Uuid::new_v4();
    log::info!("My address: {}", address);

    let mut consumer = Consumer::from_hosts(vec!["localhost:9092".to_owned()])
        .with_topic("ping".to_owned())
        .with_fallback_offset(FetchOffset::Earliest)
        .with_group("my_consumer_group".to_owned())
        .create()?;

    let mut producer = Producer::from_hosts(vec!["localhost:9092".to_owned()])
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .create()?;

    let mut stm_map = HashMap::new();

    loop {
        for msg_result in consumer.poll()?.iter() {
            for msg in msg_result.messages() {
                let ping: Ping =
                    serde_json::from_slice(msg.value).expect("failed to deser JSON to Ping");
                if ping.envelope.is_directed_at(address) {
                    let state: Box<dyn State<Types> + Send> =
                        Box::new(ListeningForPing::new(address));

                    let mut state_machine_runner = TimeBoundStateMachineRunner::new(
                        format!(
                            "Pong:{}",
                            if let Address::Direct(sender) = ping.envelope.sender {
                                sender
                            } else {
                                panic!("Sender must be a Direct address!")
                            }
                        ),
                        state,
                        Duration::from_secs(5),
                    );

                    let (outgoing, result) = state_machine_runner.run();

                    state_machine_runner
                        .deliver(InboundMessage::PingReceived(ping.clone()))
                        .unwrap();

                    stm_map.insert(
                        ping.envelope.correlation_id,
                        (state_machine_runner, outgoing, result),
                    );
                } else {
                    log::debug!("Dropped: {:?}", ping);
                }
            }
            consumer.consume_messageset(msg_result)?;
        }
        consumer.commit_consumed()?;

        let ids_to_remove = {
            let mut ids_to_remove = Vec::new();
            for (correlation_id, (stm, outgoing, result)) in stm_map.iter_mut() {
                tokio::select! {
                    Some(messages) = outgoing.recv() => {
                        for pong in messages.into_iter() {
                            log::info!("Send Pong: {:?}", pong);
                            let pong_message = serde_json::to_string_pretty(&pong)?;
                            let pong_record = Record::from_value("pong", pong_message);
                            // alternatively use producer.send_all for better performance
                            producer.send(&pong_record)?;
                            stm.deliver(InboundMessage::PongSent(pong)).unwrap();
                        }
                    }
                    res = result => {
                        let res = res
                            .expect("Result from State Machine must be communicated")
                            .unwrap_or_else(|_| panic!("State machine did not complete in time"));
                        log::info!("State machine ended at: <{}>", res.desc());
                        ids_to_remove.push(*correlation_id);
                    }
                }
            }
            ids_to_remove
        };

        for id in ids_to_remove.iter() {
            stm_map.remove(id);
        }
    }
}
