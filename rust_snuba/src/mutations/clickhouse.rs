use std::time::Duration;

use rust_arroyo::processing::strategies::run_task_in_threads::{
    RunTaskError, RunTaskFunc, TaskRunner,
};
use rust_arroyo::types::Message;
use serde::Serialize;

use reqwest::header::{HeaderMap, HeaderValue, ACCEPT_ENCODING, CONNECTION};
use reqwest::{Client, ClientBuilder};

use crate::mutations::parser::MutationBatch;
use crate::processors::eap_spans::{AttributeMap, PrimaryKey, ATTRS_SHARD_FACTOR};

#[derive(Clone)]
pub struct ClickhouseWriter {
    url: String,
    table: String,
    client: Client,
}

impl ClickhouseWriter {
    pub fn new(
        hostname: &str,
        http_port: u16,
        table: &str,
        database: &str,
        clickhouse_user: &str,
        clickhouse_password: &str,
        batch_write_timeout: Option<Duration>,
    ) -> Self {
        let mut headers = HeaderMap::with_capacity(5);
        headers.insert(CONNECTION, HeaderValue::from_static("keep-alive"));
        headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip,deflate"));
        headers.insert(
            "X-Clickhouse-User",
            HeaderValue::from_str(clickhouse_user).unwrap(),
        );
        headers.insert(
            "X-ClickHouse-Key",
            HeaderValue::from_str(clickhouse_password).unwrap(),
        );
        headers.insert(
            "X-ClickHouse-Database",
            HeaderValue::from_str(database).unwrap(),
        );

        let url = format!("http://{hostname}:{http_port}");

        let mut client_builder = ClientBuilder::new().default_headers(headers);

        if let Some(timeout) = batch_write_timeout {
            client_builder = client_builder.timeout(timeout);
        }

        Self {
            url,
            table: table.to_owned(),
            client: client_builder.build().unwrap(),
        }
    }

    async fn process_message(&self, message: &Message<MutationBatch>) -> anyhow::Result<()> {
        let body = format_query(&self.table, message.payload());

        self.client
            .post(&self.url)
            .body(body)
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }
}

impl TaskRunner<MutationBatch, (), anyhow::Error> for ClickhouseWriter {
    fn get_task(&self, message: Message<MutationBatch>) -> RunTaskFunc<(), anyhow::Error> {
        let slf = self.clone();

        Box::pin(async move {
            slf.process_message(&message)
                .await
                .map_err(RunTaskError::Other)?;
            message.try_map(|_| Ok(()))
        })
    }
}

fn format_query(table: &str, batch: &MutationBatch) -> Vec<u8> {
    let mut attr_columns = String::new();
    // attr_combined_columns is intentionally ordered the same as the clickhouse schema.
    // INSERT INTO .. SELECT FROM .. matches up columns by position only, and ignores names.
    // subqueries don't have this property.
    let mut attr_combined_columns = String::new();
    for i in 0..ATTRS_SHARD_FACTOR {
        attr_columns.push_str(&format!(",attr_str_{i} Map(String, String)"));

        attr_combined_columns.push_str(&format!(
            ",mapUpdate(old_data.attr_str_{i}, new_data.attr_str_{i}) AS attr_str_{i}"
        ));
    }

    for i in 0..ATTRS_SHARD_FACTOR {
        attr_columns.push_str(&format!(",attr_num_{i} Map(String, Float64)"));
        attr_combined_columns.push_str(&format!(
            ",mapUpdate(old_data.attr_num_{i}, new_data.attr_num_{i}) AS attr_num_{i}"
        ));
    }

    // rewriting the update query
    // WITH () as new_data
    // INSERT INTO table
    // SELECT .... updateMap stuff
    // WHERE old = new

    // filter for org_id first
    // then do the join

    // PREWHERE for making the query faster

    // another join for getting the cancellation
    // JOIN ON VALUES (x) (1) (-1)

    // Async inserts ??

    let mut body = format!(
        "
        INSERT INTO {table}
        SELECT old_data.* EXCEPT ('attr_.*') {attr_combined_columns}
        FROM {table} old_data
        JOIN (SELECT * FROM input(
            'organization_id UInt64, trace_id UUID, span_id UInt64, _sort_timestamp DateTime {attr_columns}'
        )) new_data
        ON old_data.organization_id = new_data.organization_id
            and old_data.trace_id = new_data.trace_id
            and old_data.span_id = new_data.span_id
            and old_data._sort_timestamp = new_data._sort_timestamp
            and old_data.sign = 1

        FORMAT JSONEachRow\n"
    )
    .into_bytes();

    for (filter, update) in &batch.0 {
        let mut attributes = AttributeMap::default();
        for (k, v) in &update.attr_str {
            attributes.insert_str(k.clone(), v.clone());
        }

        for (k, v) in &update.attr_num {
            attributes.insert_num(k.clone(), *v);
        }

        let row = MutationRow {
            filter: filter.clone(),
            attributes,
        };

        serde_json::to_writer(&mut body, &row).unwrap();
        body.push(b'\n');
    }

    body
}

#[derive(Serialize, Default)]
struct MutationRow {
    #[serde(flatten)]
    attributes: AttributeMap,

    #[serde(flatten)]
    filter: PrimaryKey,
}
