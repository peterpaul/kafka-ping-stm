use kafka_ping_stm::{
    setup_tracing, Address, Envelope, PartyId, Ping, Pong, Spanned, SpannedMessage,
};

use kafka::consumer::{Consumer, FetchOffset};
use kafka::producer::{Producer, Record, RequiredAcks};
use oblivious_state_machine::state::BoxedState;
use oblivious_state_machine::{
    state::{DeliveryStatus, State, StateTypes, Transition},
    state_machine::TimeBoundStateMachineRunner,
};
use std::collections::HashMap;
use std::time::Duration;
use tracing::info_span;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

#[derive(Debug)]
enum InboundMessage {
    PingReceived(Envelope<Ping>),
    PongSent(Envelope<Pong>),
}

#[derive(Debug)]
struct Types;
impl StateTypes for Types {
    type In = InboundMessage;
    type Out = Spanned<Envelope<Pong>>;
    type Err = String;
}

#[derive(Debug)]
struct ListeningForPing {
    span: tracing::Span,
    my_address: Uuid,
    received_ping: Option<Envelope<Ping>>,
}

impl ListeningForPing {
    fn new(span: tracing::Span, my_address: Uuid) -> Self {
        Self {
            span,
            my_address,
            received_ping: None,
        }
    }

    #[tracing::instrument(skip(self), fields(state = self.desc()))]
    fn receive_ping(&mut self, ping: Envelope<Ping>) {
        log::info!("Received Ping: {:?}", ping);
        self.received_ping = Some(ping);
    }
}

impl State<Types> for ListeningForPing {
    fn desc(&self) -> String {
        "Waiting for Ping".to_owned()
    }

    #[tracing::instrument(parent = &self.span, skip(self), fields(state = self.desc()))]
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

    #[tracing::instrument(parent = &self.span, skip(self), fields(state = self.desc()))]
    fn advance(&self) -> Result<Transition<Types>, <Types as StateTypes>::Err> {
        let next = match &self.received_ping {
            Some(ping) => Transition::Next(Box::new(SendingPong::new(
                self.span.clone(),
                self.my_address,
                ping.clone(),
            ))),
            None => Transition::Same,
        };
        Ok(next)
    }
}

#[derive(Debug)]
struct SendingPong {
    span: tracing::Span,
    my_address: Uuid,
    received_ping: Envelope<Ping>,
    sent_pong: Option<Envelope<Pong>>,
}

impl SendingPong {
    fn new(span: tracing::Span, my_address: Uuid, received_ping: Envelope<Ping>) -> Self {
        Self {
            span,
            my_address,
            received_ping,
            sent_pong: None,
        }
    }

    #[tracing::instrument(skip(self), fields(state = self.desc()))]
    fn get_pong(&self) -> Envelope<Pong> {
        let pong = Envelope::new(
            PartyId(self.my_address),
            Address::Single(self.received_ping.source()),
            Pong::new(self.received_ping.body().session_id()),
        );
        log::info!("Pong to send: {:?}", pong);
        pong
    }
}

impl State<Types> for SendingPong {
    fn desc(&self) -> String {
        "Sending Pong".to_owned()
    }

    #[tracing::instrument(parent = &self.span, skip(self), fields(state = self.desc()))]
    fn initialize(&self) -> Vec<<Types as StateTypes>::Out> {
        vec![Spanned::new_cloned(&self.span, self.get_pong())]
    }

    #[tracing::instrument(parent = &self.span, skip(self), fields(state = self.desc()))]
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

    #[tracing::instrument(parent = &self.span, skip(self), fields(state = self.desc()))]
    fn advance(&self) -> Result<Transition<Types>, <Types as StateTypes>::Err> {
        let next = match &self.sent_pong {
            Some(_pong) => Transition::Next(Box::new(SentPong::new(self.span.clone()))),
            None => Transition::Same,
        };
        Ok(next)
    }
}

#[derive(Debug)]
struct SentPong {
    span: tracing::Span,
}

impl SentPong {
    fn new(span: tracing::Span) -> Self {
        Self { span }
    }
}

impl State<Types> for SentPong {
    fn desc(&self) -> String {
        "Sent Pong".to_owned()
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
        Ok(Transition::Terminal)
    }
}

fn kafka_send_messages(
    producer: &mut Producer,
    messages: Vec<<Types as StateTypes>::Out>,
    stm: &mut TimeBoundStateMachineRunner<Types>,
) {
    for pong in messages.into_iter() {
        let pong_span = tracing::info_span!(parent: pong.span(), "send_pong");
        pong_span.in_scope(|| {
            log::info!("Send Pong: {:?}", pong);
            let pong_message = serde_json::to_string_pretty(&pong.inner()).unwrap();
            let pong_record = Record::from_value("pong", pong_message);
            // alternatively use producer.send_all for better performance
            producer.send(&pong_record).unwrap();
            stm.deliver(InboundMessage::PongSent(pong.unwrap()))
                .unwrap();
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    setup_tracing("pong")?;

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
        log::debug!("in consumer loop");
        for msg_result in consumer.poll()?.iter() {
            log::debug!("polled messages: {}", msg_result.messages().len());
            for msg in msg_result.messages() {
                let ping: Envelope<SpannedMessage<Ping>> =
                    serde_json::from_slice(msg.value).expect("failed to deser JSON to Ping");
                log::debug!("Incoming ping message: {:?}", ping);
                if ping.is_directed_at(address) {
                    let context = ping.body().context().extract();

                    let span = info_span!("Pong span");
                    span.set_parent(context);

                    let _ = span.enter();
                    let state: BoxedState<Types> = Box::new(ListeningForPing::new(span, address));

                    let mut state_machine_runner = TimeBoundStateMachineRunner::new(
                        format!("Pong:{}", ping.source().0),
                        state,
                        Duration::from_secs(5),
                    );

                    let (outgoing, result) = state_machine_runner.run();

                    let ping_envelope = ping.map_body(|ping| ping.unwrap());
                    state_machine_runner
                        .deliver(InboundMessage::PingReceived(ping_envelope.clone()))
                        .unwrap();

                    stm_map.insert(
                        ping_envelope.body().session_id(),
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
            for (session_id, (stm, outgoing, result)) in stm_map.iter_mut() {
                tokio::select! {
                    Some(messages) = outgoing.recv() => {
                        kafka_send_messages(&mut producer, messages, stm);
                    }
                    res = result => {
                        let res = res
                            .expect("Result from State Machine must be communicated")
                            .unwrap_or_else(|_| panic!("State machine did not complete in time"));
                        log::info!("State machine ended at: <{}>", res.desc());
                        ids_to_remove.push(*session_id);
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
