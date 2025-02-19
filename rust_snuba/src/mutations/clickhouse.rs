use sentry_arroyo::counter;
use sentry_arroyo::processing::strategies::run_task_in_threads::{
    RunTaskError, RunTaskFunc, TaskRunner,
};
use sentry_arroyo::types::Message;
use serde::Serialize;
use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderValue, ACCEPT_ENCODING, CONNECTION};
use reqwest::{Client, ClientBuilder};
use uuid::Uuid;

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
        let queries = format_query(&self.table, message.payload());
        self.run_queries(queries).await
    }

    async fn run_queries(&self, queries: Vec<Vec<u8>>) -> anyhow::Result<()> {
        let session_id = Uuid::new_v4().to_string();

        let mut session_check = false;

        for query in queries {
            let mut request = self.client.post(&self.url).query(&[
                // ensure that the tables from CREATE TEMPORARY TABLE exist in subsequent
                // queries
                ("session_id", session_id.as_str()),
                // ensure that HTTP status code is correct
                // https://clickhouse.com/docs/en/interfaces/http#http_response_codes_caveats
                // TODO: port to main consumer
                ("wait_end_of_query", "1"),
            ]);

            // ensure that we are in a session. if some load balancer drops the querystring, we
            // want to know.
            // on first query, run without session_check. after that, the session should exist.
            if session_check {
                request = request.query(&[("session_check", "1")]);
            }

            session_check = true;

            let response = request.body(query).send().await?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await;
                anyhow::bail!(
                    "bad response while inserting mutation, status: {}, response body: {:?}",
                    status,
                    body
                );
            }
        }

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

fn format_query(table: &str, batch: &MutationBatch) -> Vec<Vec<u8>> {
    let mut attr_columns = String::new();
    // attr_combined_columns is intentionally ordered the same as the clickhouse schema.
    // INSERT INTO .. SELECT FROM .. matches up columns by position only, and ignores names.
    // subqueries don't have this property.
    let mut attr_combined_columns = String::new();
    for i in 0..ATTRS_SHARD_FACTOR {
        attr_columns.push_str(&format!(",attr_str_{i} Map(String, String)"));

        attr_combined_columns.push_str(&format!(
            ",if(sign = 1, mapUpdate(old_data.attr_str_{i}, new_data.attr_str_{i}), old_data.attr_str_{i}) AS attr_str_{i}"
        ));
    }

    for i in 0..ATTRS_SHARD_FACTOR {
        attr_columns.push_str(&format!(",attr_num_{i} Map(String, Float64)"));
        attr_combined_columns.push_str(&format!(
            ",if(sign = 1, mapUpdate(old_data.attr_num_{i}, new_data.attr_num_{i}), old_data.attr_num_{i}) AS attr_num_{i}"
        ));
    }

    let input_schema = format!("organization_id UInt64, _sort_timestamp DateTime, trace_id UUID, span_id UInt64 {attr_columns}");
    let create_tmp_table = format!("CREATE TEMPORARY TABLE new_data ({input_schema})").into_bytes();
    let mut insert_tmp_table = "INSERT INTO new_data FORMAT JSONEachRow\n"
        .to_owned()
        .into_bytes();

    counter!("eap_mutations.mutation_rows", batch.0.len() as u64);
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

        serde_json::to_writer(&mut insert_tmp_table, &row).unwrap();
        insert_tmp_table.push(b'\n');
    }

    let main_insert = format!(
        "
        INSERT INTO {table}
        SELECT old_data.* EXCEPT ('sign|attr_.*'), arrayJoin([-1, 1]) as sign {attr_combined_columns}
        FROM {table} old_data
        GLOBAL JOIN new_data
        ON old_data.organization_id = new_data.organization_id
            and old_data.trace_id = new_data.trace_id
            and old_data.span_id = new_data.span_id
            and old_data._sort_timestamp = new_data._sort_timestamp
        PREWHERE
        (old_data.organization_id,
            old_data._sort_timestamp,
            old_data.trace_id,
            old_data.span_id,
            ) GLOBAL IN (SELECT organization_id, _sort_timestamp, trace_id, span_id FROM new_data)
        WHERE
        old_data.sign = 1
        "
    )
    .into_bytes();

    let drop_tmp_table = "DROP TABLE IF EXISTS new_data".to_owned().into_bytes();

    vec![
        create_tmp_table,
        insert_tmp_table,
        main_insert,
        drop_tmp_table,
    ]
}

#[derive(Serialize, Default)]
struct MutationRow {
    #[serde(flatten)]
    attributes: AttributeMap,

    #[serde(flatten)]
    filter: PrimaryKey,
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::time::{SystemTime, UNIX_EPOCH};
    use uuid::Uuid;

    use crate::mutations::parser::Update;

    use super::*;

    struct ClickhouseTestClient {
        url: String,
        table: String,
        client: Client,
    }

    impl ClickhouseTestClient {
        pub async fn new(table: String) -> anyhow::Result<Self> {
            let hostname = env::var("CLICKHOUSE_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
            let http_port = 8123;
            let url = format!("http://{hostname}:{http_port}");
            let uuid = Uuid::new_v4().to_string();
            let table_id = uuid.split('-').next().unwrap();

            let client = reqwest::Client::new();
            let test_table = format!("{table}_{table_id}_local\n");

            let body =
                format!("CREATE TABLE IF NOT EXISTS {test_table} AS {table}_local\n").into_bytes();

            // use client to create a new table locally
            client.post(url.clone()).body(body).send().await?;

            Ok(Self {
                url,
                table: test_table,
                client,
            })
        }

        pub async fn run_mutation(&self, queries: Vec<Vec<u8>>) -> anyhow::Result<()> {
            let writer = ClickhouseWriter {
                url: self.url.clone(),
                table: self.table.clone(),
                client: self.client.clone(),
            };

            writer.run_queries(queries).await
        }

        pub async fn select_final(&self) -> anyhow::Result<String> {
            let table = &self.table;
            let final_query = format!("SELECT * FROM {table} FINAL\n").into_bytes();

            let response = self.client.post(&self.url).body(final_query).send().await?;

            let update = response.text().await?;

            Ok(update)
        }

        pub async fn drop_table(&self) -> anyhow::Result<()> {
            let table = &self.table;
            let drop = format!("DROP TABLE {table}\n").into_bytes();

            self.client.post(&self.url).body(drop).send().await?;

            Ok(())
        }
    }

    #[tokio::test]
    async fn test_simple_mutation() {
        let mut batch = MutationBatch::default();
        let mut update = Update::default();

        let organization_id = 69;
        let curr_time_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        let _sort_timestamp = curr_time_unix as u32; //TODO year 2038 problem
        let trace_id = Uuid::parse_str("deadbeef-dead-beef-dead-beefdeadbeef").unwrap();
        let span_id = 16045690984833335023;

        let primary_key = PrimaryKey {
            organization_id,
            _sort_timestamp,
            trace_id,
            span_id,
        };
        update.attr_str.insert("a".to_string(), "b".to_string());

        // build the mutation batch
        batch.0.insert(primary_key.clone(), update);

        let test_client = ClickhouseTestClient::new("eap_spans_2".to_string())
            .await
            .unwrap();

        let test_table = &test_client.table;

        let insert =
        format!("INSERT INTO {test_table} (organization_id, _sort_timestamp, trace_id, span_id, sign, retention_days) VALUES ({organization_id}, {_sort_timestamp}, \'{trace_id}\', {span_id}, 1, 90)\n").into_bytes();

        let _ = test_client
            .client
            .post(&test_client.url)
            .body(insert)
            .send()
            .await;

        let all_queries = format_query(test_table, &batch);
        let _ = test_client.run_mutation(all_queries).await;

        // merge data at query time for up-to-date results
        let mutation = test_client.select_final().await;
        assert!(mutation.unwrap().contains("{'a':'b'}"));

        // clean up the temporary table at the end of test
        let _ = test_client.drop_table().await;
    }

    #[test]
    fn format_query_snap() {
        let mut batch = MutationBatch::default();
        let mut update = Update::default();
        let primary_key = PrimaryKey {
            organization_id: 69,
            _sort_timestamp: 1727466947,
            trace_id: Uuid::parse_str("deadbeef-dead-beef-dead-beefdeadbeef").unwrap(),
            span_id: 16045690984833335023,
        };
        update.attr_str.insert("a".to_string(), "b".to_string());

        batch.0.insert(primary_key.clone(), update);

        let mut snapshot = String::new();
        for query in format_query("eap_spans_local", &batch) {
            snapshot.push_str(std::str::from_utf8(&query).unwrap());
            snapshot.push_str(";\n");
        }

        insta::assert_snapshot!(snapshot);
    }
}
