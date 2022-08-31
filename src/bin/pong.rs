use kafka_ping_stm::{Address, Envelope, PartyId, Ping, Pong};

use kafka::consumer::{Consumer, FetchOffset};
use kafka::producer::{Producer, Record, RequiredAcks};
use oblivious_state_machine::state::BoxedState;
use oblivious_state_machine::{
    state::{DeliveryStatus, State, StateTypes, Transition},
    state_machine::TimeBoundStateMachineRunner,
};
use opentelemetry::global;
use std::collections::HashMap;
use std::time::Duration;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};
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
    type Out = Envelope<Pong>;
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

    #[tracing::instrument]
    fn receive_ping(&mut self, ping: Envelope<Ping>) {
        log::info!("Received Ping: {:?}", ping);
        self.received_ping = Some(ping);
    }
}

impl State<Types> for ListeningForPing {
    fn desc(&self) -> String {
        "Waiting for Ping".to_owned()
    }

    #[tracing::instrument(parent = &self.span)]
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

    #[tracing::instrument(parent = &self.span)]
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

    #[tracing::instrument]
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

    #[tracing::instrument(parent = &self.span)]
    fn initialize(&self) -> Vec<Envelope<Pong>> {
        vec![self.get_pong()]
    }

    #[tracing::instrument(parent = &self.span)]
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

    #[tracing::instrument(parent = &self.span)]
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

    #[tracing::instrument(parent = &self.span)]
    fn deliver(
        &mut self,
        message: <Types as StateTypes>::In,
    ) -> DeliveryStatus<<Types as StateTypes>::In, <Types as StateTypes>::Err> {
        DeliveryStatus::Unexpected(message)
    }

    #[tracing::instrument(parent = &self.span)]
    fn advance(&self) -> Result<Transition<Types>, <Types as StateTypes>::Err> {
        Ok(Transition::Terminal)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    // Allows you to pass along context (i.e., trace IDs) across services
    global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());
    // Sets up the machinery needed to export data to Jaeger
    // There are other OTel crates that provide pipelines for the vendors
    // mentioned earlier.
    let tracer = opentelemetry_jaeger::new_pipeline()
        .with_service_name("ping")
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
                let ping: Envelope<Ping> =
                    serde_json::from_slice(msg.value).expect("failed to deser JSON to Ping");
                log::debug!("Incoming ping message: {:?}", ping);
                if ping.is_directed_at(address) {
                    let span = tracing::info_span!("Pong span");
                    let _ = span.enter();
                    let state: BoxedState<Types> = Box::new(ListeningForPing::new(span, address));

                    let mut state_machine_runner = TimeBoundStateMachineRunner::new(
                        format!("Pong:{}", ping.source().0),
                        state,
                        Duration::from_secs(5),
                    );

                    let (outgoing, result) = state_machine_runner.run();

                    state_machine_runner
                        .deliver(InboundMessage::PingReceived(ping.clone()))
                        .unwrap();

                    stm_map.insert(
                        ping.body().session_id(),
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
