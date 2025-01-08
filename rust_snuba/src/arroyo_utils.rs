use sentry_arroyo::processing::strategies::InvalidMessage;
/// Some helper functions that work around Arroyo's ergonomics, and should eventually make it into
/// Arroyo
use sentry_arroyo::types::{InnerMessage, Message};

pub fn invalid_message_err<T>(message: &Message<T>) -> Result<InvalidMessage, anyhow::Error> {
    let InnerMessage::BrokerMessage(ref msg) = message.inner_message else {
        return Err(anyhow::anyhow!("Unexpected message type"));
    };

    let err = InvalidMessage {
        partition: msg.partition,
        offset: msg.offset,
    };

    Ok(err)
}
