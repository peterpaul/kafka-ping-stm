use kafka_ping_stm::{
    kafka_send_message_with_span, spawn_kafka_consumer_task, try_init_tracing, Ping, Pong,
};

use kafka::consumer::{Consumer, FetchOffset};
use kafka::producer::{Producer, RequiredAcks};
use oblivious_state_machine::state::BoxedState;
use oblivious_state_machine::{
    state::{DeliveryStatus, State, StateTypes, Transition},
    state_machine::{Either, TimeBoundStateMachineResult, TimeBoundStateMachineRunner},
};
use std::{fmt::Debug, time::Duration};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

#[derive(Debug)]
struct Types;
impl StateTypes for Types {
    type In = Pong;
    type Out = Ping;
    type Err = String;
}

#[derive(Debug)]
struct PingState {
    ping_to_send: Ping,
    received_pong: Option<Pong>,
}

impl PingState {
    fn new(ping_to_send: Ping) -> Self {
        Self {
            ping_to_send,
            received_pong: None,
        }
    }

    #[tracing::instrument(skip(self), fields(state = self.desc()))]
    fn receive_pong(&mut self, pong: Pong) {
        assert!(pong.session_id() == self.ping_to_send.session_id());
        log::debug!("Received Pong: {:?}", pong);
        self.received_pong = Some(pong);
    }
}

impl State<Types> for PingState {
    fn desc(&self) -> String {
        "Ping State".to_owned()
    }

    #[tracing::instrument(skip(self), fields(state = self.desc()), ret)]
    fn initialize(&self) -> Vec<Ping> {
        vec![self.ping_to_send.clone()]
    }

    #[tracing::instrument(skip(self), fields(state = self.desc()), ret)]
    fn deliver(&mut self, message: Pong) -> DeliveryStatus<Pong, String> {
        self.receive_pong(message);
        DeliveryStatus::Delivered
    }

    #[tracing::instrument(skip(self), fields(state = self.desc()), ret, err)]
    fn advance(&self) -> Result<Transition<Types>, String> {
        Ok(match &self.received_pong {
            Some(_pong) => Transition::Terminal,
            None => Transition::Same,
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    try_init_tracing("ping")?;

    let mut producer = Producer::from_hosts(vec!["localhost:9092".to_owned()])
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .create()?;

    let consumer = Consumer::from_hosts(vec!["localhost:9092".to_owned()])
        .with_topic("pong".to_owned())
        .with_fallback_offset(FetchOffset::Earliest)
        .with_group("my_consumer_group".to_owned())
        .create()?;

    let ping_span = tracing::info_span!("ping span");

    let session_id = Uuid::new_v4();
    let ping_to_send = Ping::new(session_id);
    let initial_state: BoxedState<Types> = Box::new(PingState::new(ping_to_send));

    let mut state_machine_runner = TimeBoundStateMachineRunner::new(
        format!("Ping:{}", session_id).into(),
        initial_state,
        Duration::from_secs(15),
        ping_span,
    );

    let (state_machine_tx, mut state_machine_rx) = mpsc::unbounded_channel();

    state_machine_runner.run(state_machine_tx);

    let (kafka_receiver_task, mut incoming, shutdown_kafka_receiver_task): (
        tokio::task::JoinHandle<()>,
        mpsc::UnboundedReceiver<Pong>,
        oneshot::Sender<()>,
    ) = spawn_kafka_consumer_task(consumer);

    let res: TimeBoundStateMachineResult<Types> = loop {
        log::trace!("polling futures in order to advance STM");
        tokio::select! {
            Some(pong) = incoming.recv() => {
                log::trace!("Incoming pong message: {:?}", pong);
                if pong.session_id() == session_id {
                    state_machine_runner.deliver(pong).unwrap();
                }
            }
            Some(stm_event) = state_machine_rx.recv() => {
                match stm_event {
                    Either::Messages { messages, span, .. } => {
                        log::trace!("messages to send: {}", messages.len());
                        for ping in messages {
                            kafka_send_message_with_span(&mut producer, "ping", ping, span.clone())?;
                        }
                    }
                    Either::Result { result, .. } => {
                        log::trace!("State machine yielded result");
                        break result;
                    }
                }
            }
        }
        log::trace!("consumer loop end.");
    };

    let terminal_state = res.unwrap_or_else(|_| panic!("State machine did not complete in time"));

    log::debug!("State machine ended at: <{}>", terminal_state.desc());

    shutdown_kafka_receiver_task.send(()).unwrap();
    kafka_receiver_task.await.map_err(|e| e.into())
}
