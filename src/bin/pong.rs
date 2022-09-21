use kafka_ping_stm::{
    kafka_send_message, spawn_kafka_consumer_task, try_init_tracing, Ping, Pong, SpannedMessage,
};

use kafka::consumer::{Consumer, FetchOffset};
use kafka::producer::{Producer, RequiredAcks};
use oblivious_state_machine::state::BoxedState;
use oblivious_state_machine::{
    state::{DeliveryStatus, State, StateTypes, Transition},
    state_machine::{Either, StateMachineId, StateMachineTx, TimeBoundStateMachineRunner},
};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::info_span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

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

type StateMachineMap = HashMap<StateMachineId, TimeBoundStateMachineRunner<Types>>;

#[derive(Debug)]
struct ListeningForPing {
    received_ping: Option<Ping>,
}

impl ListeningForPing {
    fn new() -> Self {
        Self {
            received_ping: None,
        }
    }

    #[tracing::instrument(skip(self), fields(state = self.desc()))]
    fn receive_ping(&mut self, ping: Ping) {
        log::debug!("Received Ping: {:?}", ping);
        self.received_ping = Some(ping);
    }
}

impl State<Types> for ListeningForPing {
    fn desc(&self) -> String {
        "Waiting for Ping".to_owned()
    }

    #[tracing::instrument(skip(self), fields(state = self.desc()), ret)]
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

    #[tracing::instrument(skip(self), fields(state = self.desc()), ret, err)]
    fn advance(&self) -> Result<Transition<Types>, <Types as StateTypes>::Err> {
        Ok(match &self.received_ping {
            Some(ping) => Transition::Next(Box::new(SendingPong::new(ping.clone()))),
            None => Transition::Same,
        })
    }
}

#[derive(Debug)]
struct SendingPong {
    received_ping: Ping,
    sent_pong: Option<Pong>,
}

impl SendingPong {
    fn new(received_ping: Ping) -> Self {
        Self {
            received_ping,
            sent_pong: None,
        }
    }

    #[tracing::instrument(skip(self), fields(state = self.desc()))]
    fn get_pong(&self) -> Pong {
        let pong = Pong::new(self.received_ping.session_id());
        log::debug!("Pong to send: {:?}", pong);
        pong
    }
}

impl State<Types> for SendingPong {
    fn desc(&self) -> String {
        "Sending Pong".to_owned()
    }

    #[tracing::instrument(skip(self), fields(state = self.desc()), ret)]
    fn initialize(&self) -> Vec<<Types as StateTypes>::Out> {
        vec![self.get_pong()]
    }

    #[tracing::instrument(skip(self), fields(state = self.desc()), ret)]
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

    #[tracing::instrument(skip(self), fields(state = self.desc()), ret, err)]
    fn advance(&self) -> Result<Transition<Types>, <Types as StateTypes>::Err> {
        Ok(match &self.sent_pong {
            Some(_pong) => Transition::Terminal,
            None => Transition::Same,
        })
    }
}

fn start_new_stm(
    ping: SpannedMessage<Ping>,
    state_machine_tx: StateMachineTx<Types>,
) -> (StateMachineId, TimeBoundStateMachineRunner<Types>) {
    log::trace!("Incoming ping message: {:?}", ping);

    let span = info_span!("pong span");
    let parent_context = ping.context().extract();
    span.set_parent(parent_context);

    let ping: Ping = ping.unwrap();

    let initial_state: BoxedState<Types> = Box::new(ListeningForPing::new());

    let stm_id: StateMachineId = format!("Pong:{}", ping.session_id()).into();

    let mut state_machine_runner = TimeBoundStateMachineRunner::new(
        stm_id.clone(),
        initial_state,
        Duration::from_secs(5),
        span,
    );

    state_machine_runner.run(state_machine_tx);

    state_machine_runner
        .deliver(InboundMessage::PingReceived(ping))
        .unwrap();

    (stm_id, state_machine_runner)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    try_init_tracing("pong")?;

    let consumer = Consumer::from_hosts(vec!["localhost:9092".to_owned()])
        .with_topic("ping".to_owned())
        .with_fallback_offset(FetchOffset::Earliest)
        .with_group("my_consumer_group".to_owned())
        .create()?;

    let mut producer = Producer::from_hosts(vec!["localhost:9092".to_owned()])
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .create()?;

    let mut stm_map: StateMachineMap = HashMap::new();

    let (_kafka_consumer_task, mut ping_rx, _shutdown_tx): (
        tokio::task::JoinHandle<()>,
        mpsc::UnboundedReceiver<SpannedMessage<Ping>>,
        oneshot::Sender<()>,
    ) = spawn_kafka_consumer_task(consumer);

    let (state_machine_tx, mut state_machine_rx) = mpsc::unbounded_channel();

    loop {
        log::trace!("Main event loop");
        tokio::select! {
            Some(ping) = ping_rx.recv() => {
                let (stm_id, stm_runner) = start_new_stm(ping, state_machine_tx.clone());
                stm_map.insert(stm_id, stm_runner);
            }
            Some(stm_event) = state_machine_rx.recv() => {
                match stm_event {
                    Either::Messages { messages, from, span } => {
                        span.in_scope(|| {
                            let stm = stm_map.get(&from).unwrap();
                            for pong in messages {
                                kafka_send_message(&mut producer, "pong", pong.clone()).unwrap();
                                stm.deliver(InboundMessage::PongSent(pong)).unwrap();
                            }
                        });
                    }
                    Either::Result { result, from, .. } => {
                        let final_state = result.unwrap_or_else(|_| panic!("State Machine should never fail."));
                        log::debug!("State machine ended at: <{}>", final_state.desc());
                        stm_map.remove(&from);
                    }
                }
            }
        };
    }
}
