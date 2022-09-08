use kafka_ping_stm::{
    setup_tracing, Address, Envelope, PartyId, Ping, Pong, PropagationContext, Spanned,
    SpannedMessage,
};

use kafka::consumer::{Consumer, FetchOffset};
use kafka::producer::{Producer, Record, RequiredAcks};
use oblivious_state_machine::{
    state::{DeliveryStatus, State, StateTypes, Transition},
    state_machine::{TimeBoundStateMachineResult, TimeBoundStateMachineRunner},
};
use std::time::Duration;
use tokio::sync::{mpsc, mpsc::UnboundedSender, oneshot};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

#[derive(Debug)]
struct Types;
impl StateTypes for Types {
    type In = Envelope<Pong>;
    type Out = Spanned<Envelope<Ping>>;
    type Err = String;
}

#[derive(Debug)]
struct SendingPing {
    span: tracing::Span,
    ping_to_send: Envelope<Ping>,
}

impl SendingPing {
    fn new(span: tracing::Span, ping_to_send: Envelope<Ping>) -> Self {
        Self { span, ping_to_send }
    }
}

impl State<Types> for SendingPing {
    fn desc(&self) -> String {
        "Sending Ping".to_owned()
    }

    #[tracing::instrument(parent = &self.span, skip(self), fields(state = self.desc()))]
    fn initialize(&self) -> Vec<<Types as StateTypes>::Out> {
        vec![Spanned::new_cloned(&self.span, self.ping_to_send.clone())]
    }

    #[tracing::instrument(parent = &self.span, skip(self), fields(state = self.desc()))]
    fn deliver(
        &mut self,
        message: <Types as StateTypes>::In,
    ) -> DeliveryStatus<<Types as StateTypes>::In, <Types as StateTypes>::Err> {
        DeliveryStatus::Unexpected(message)
    }

    #[tracing::instrument(parent = &self.span, skip(self), fields(state = self.desc()))]
    fn advance(&self) -> Result<Transition<Types>, <Types as StateTypes>::Err> {
        Ok(Transition::Next(Box::new(ListeningForPong::new(
            self.span.clone(),
            self.ping_to_send.clone(),
        ))))
    }
}

#[derive(Debug)]
struct ListeningForPong {
    span: tracing::Span,
    sent_ping: Envelope<Ping>,
    received_pong: Option<<Types as StateTypes>::In>,
}

impl ListeningForPong {
    fn new(span: tracing::Span, sent_ping: Envelope<Ping>) -> Self {
        Self {
            span,
            sent_ping,
            received_pong: None,
        }
    }

    #[tracing::instrument(skip(self), fields(state = self.desc()))]
    fn receive_pong(&mut self, pong: <Types as StateTypes>::In) {
        log::info!("Received Pong: {:?}", pong);
        self.received_pong = Some(pong);
    }
}

impl State<Types> for ListeningForPong {
    fn desc(&self) -> String {
        "Waiting for Pong".to_owned()
    }

    #[tracing::instrument(parent = &self.span, skip(self), fields(state = self.desc()))]
    fn deliver(
        &mut self,
        message: <Types as StateTypes>::In,
    ) -> DeliveryStatus<<Types as StateTypes>::In, <Types as StateTypes>::Err> {
        if message.body().session_id() == self.sent_ping.body().session_id() {
            self.receive_pong(message);
            DeliveryStatus::Delivered
        } else {
            DeliveryStatus::Unexpected(message)
        }
    }

    #[tracing::instrument(parent = &self.span, skip(self), fields(state = self.desc()))]
    fn advance(&self) -> Result<Transition<Types>, <Types as StateTypes>::Err> {
        let next = match &self.received_pong {
            Some(_pong) => Transition::Terminal,
            None => Transition::Same,
        };
        Ok(next)
    }
}

fn kafka_send_pings(
    producer: &mut Producer,
    pings: Vec<<Types as StateTypes>::Out>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    for ping in pings {
        let ping_span = tracing::info_span!(parent: ping.span(), "send_ping");
        ping_span.in_scope(|| {
            let context = PropagationContext::inject(&ping.span().context());

            let ping_envelope: Envelope<Ping> = ping.unwrap();
            // attach propagation context to outgoing message
            let spanned_message: Envelope<SpannedMessage<Ping>> =
                ping_envelope.map_body(|ping| SpannedMessage::new(context, ping));
            log::info!("Send Ping: {:?}", spanned_message);
            let ping_message = serde_json::to_string_pretty(&spanned_message).unwrap();
            let ping_record = Record::from_value("ping", ping_message);
            producer.send(&ping_record).unwrap();
        })
    }
    Ok(())
}

fn kafka_read_pongs(
    mut consumer: Consumer,
    address: Uuid,
    pong_tx: UnboundedSender<<Types as StateTypes>::In>,
    mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    while shutdown_rx.try_recv().is_err() {
        log::debug!("polling kafka for messages");
        for msg_result in consumer.poll()?.iter() {
            log::debug!("polled messages: {}", msg_result.messages().len());
            for msg in msg_result.messages() {
                let pong: <Types as StateTypes>::In = serde_json::from_slice(msg.value)?;
                log::debug!("Incoming pong message: {:?}", pong);
                if pong.is_directed_at(address) {
                    pong_tx.send(pong)?;
                } else {
                    log::debug!("Dropped: {:?}", pong);
                }
            }
            // This could be problematic, if the ping was directed at a specific node and there are multiple pong nodes running.
            consumer.consume_messageset(msg_result)?;
        }
        consumer.commit_consumed()?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    setup_tracing("ping")?;

    let address = Uuid::new_v4();
    log::info!("My address: {}", address);

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
    let _ = ping_span.enter();

    let state: Box<dyn State<Types> + Send> = Box::new(SendingPing::new(
        ping_span,
        Envelope::new(
            PartyId(address),
            Address::Broadcast,
            Ping::new(Uuid::new_v4()),
        ),
    ));

    let mut state_machine_runner = TimeBoundStateMachineRunner::new(
        format!("Ping:{}", address),
        state,
        Duration::from_secs(15),
    );

    let (mut outgoing, mut result) = state_machine_runner.run();

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let (pong_tx, mut pong_rx) = mpsc::unbounded_channel::<<Types as StateTypes>::In>();

    let kafka_receiver_task = tokio::task::spawn_blocking(move || {
        kafka_read_pongs(consumer, address, pong_tx, shutdown_rx).unwrap();
    });

    let res: TimeBoundStateMachineResult<Types> = loop {
        log::debug!("polling stm for messages");
        tokio::select! {
            Some(pong) = pong_rx.recv() => {
                state_machine_runner.deliver(pong).unwrap();
            }
            Some(messages) = outgoing.recv() => {
                log::debug!("messages to send: {}", messages.len());
                kafka_send_pings(&mut producer, messages)?;
            }
            res = &mut result => {
                log::debug!("State machine yielded result");
                break res.expect("Result from State Machine must be communicated");
            }
        }
        log::debug!("consumer loop end.");
    };

    let terminal_state = res.unwrap_or_else(|_| panic!("State machine did not complete in time"));

    log::info!("State machine ended at: <{}>", terminal_state.desc());

    shutdown_tx.send(()).unwrap();
    kafka_receiver_task.await?;

    Ok(())
}
