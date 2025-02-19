use std::collections::BTreeMap;

use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::counter;
use sentry_arroyo::processing::strategies::run_task_in_threads::{
    RunTaskError, RunTaskFunc, TaskRunner,
};
use sentry_arroyo::types::Message;

use schemars::JsonSchema;
use sentry::Hub;
use sentry::SentryFutureExt;
use serde::Deserialize;

use crate::arroyo_utils::invalid_message_err;
use crate::processors::eap_spans::{FromPrimaryKey, PrimaryKey};

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(crate) struct Update {
    // the update clause
    pub attr_str: BTreeMap<String, String>,
    pub attr_num: BTreeMap<String, f64>,
}

impl Update {
    pub fn merge(&mut self, other: Update) {
        self.attr_str.extend(other.attr_str);
        self.attr_num.extend(other.attr_num);
    }
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(crate) struct MutationMessage {
    // primary key, the mutation only applies on the rows that match this filter
    pub filter: FromPrimaryKey,

    // the span attributes to update
    pub update: Update,
}

#[derive(Default)]
pub(crate) struct MutationBatch(pub BTreeMap<PrimaryKey, Update>);

#[derive(Clone)]
pub(crate) struct MutationParser;

impl MutationParser {
    async fn process_message(
        self,
        message: Message<KafkaPayload>,
    ) -> Result<Message<MutationMessage>, RunTaskError<anyhow::Error>> {
        let maybe_err = RunTaskError::InvalidMessage(
            invalid_message_err(&message).map_err(RunTaskError::Other)?,
        );

        message
            .try_map(|payload| {
                let payload = payload
                    .payload()
                    .ok_or(anyhow::anyhow!("no payload in message"))?;
                let parsed: MutationMessage = serde_json::from_slice(payload)?;
                Ok(parsed)
            })
            .map_err(|error: anyhow::Error| {
                let error: &dyn std::error::Error = error.as_ref();
                counter!("invalid_mutation");
                tracing::error!(error, "failed processing mutation");
                maybe_err
            })
    }
}

impl TaskRunner<KafkaPayload, MutationMessage, anyhow::Error> for MutationParser {
    fn get_task(
        &self,
        message: Message<KafkaPayload>,
    ) -> RunTaskFunc<MutationMessage, anyhow::Error> {
        Box::pin(
            self.clone()
                .process_message(message)
                .bind_hub(Hub::new_from_top(Hub::current())),
        )
    }
}
