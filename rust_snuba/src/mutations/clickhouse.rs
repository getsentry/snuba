use std::collections::BTreeMap;
use std::time::Duration;

use rust_arroyo::processing::strategies::run_task_in_threads::{
    RunTaskError, RunTaskFunc, TaskRunner,
};
use rust_arroyo::types::Message;
use serde::{Deserialize, Serialize};

use reqwest::header::{HeaderMap, HeaderValue, ACCEPT_ENCODING, CONNECTION};
use reqwest::{Client, ClientBuilder};
use uuid::Uuid;

use crate::mutations::parser::MutationBatch;
use crate::processors::eap_spans::{AttributeMap, PrimaryKey, ATTRS_SHARD_FACTOR};

use super::parser::Update;

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
        let session_id = Uuid::new_v4().to_string();

        for query in queries {
            self.client
                .post(&self.url)
                .query(&[("session_id", &session_id)])
                .body(query)
                .send()
                .await?
                .error_for_status()?;
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

struct ClickhouseTestClient {
    url: String,
    table: String,
    client: Client,
}

impl ClickhouseTestClient {
    pub async fn new(table: String) -> anyhow::Result<Self> {
        // hardcoding local Clickhouse settings
        let hostname = "127.0.0.1";
        let http_port = 8123;
        let url = format!("http://{hostname}:{http_port}");

        let client = reqwest::Client::new();
        let test_table = format!("{table}_test_local\n");

        let body =
            format!("CREATE TABLE IF NOT EXISTS {test_table} AS {table}_local\n").into_bytes();

        // use client to create a new table in local CH
        client.post(url.clone()).body(body).send().await?;

        Ok(Self {
            url: url,
            table: test_table,
            client: client,
        })
    }

    // actually run a query in the table
    pub async fn run_mutation(&self, queries: Vec<Vec<u8>>) -> anyhow::Result<()> {
        // run the vec of queries against Clickhouse
        let session_id = Uuid::new_v4().to_string();

        for query in queries {
            self.client
                .post(&self.url)
                .query(&[("session_id", &session_id)])
                .body(query)
                .send()
                .await?;
        }
        Ok(())
    }

    pub async fn insert_data(&self, primary_key: PrimaryKey) -> anyhow::Result<()> {
        let organization_id = primary_key.organization_id;
        let _sort_timestamp = primary_key._sort_timestamp;
        let trace_id = primary_key.trace_id;
        let span_id = primary_key.span_id;
        let retention = 90;

        let table = &self.table;

        let insert =
        format!("INSERT INTO {table} (organization_id, _sort_timestamp, trace_id, span_id, sign, retention_days) VALUES ({organization_id}, {_sort_timestamp}, \'{trace_id}\', {span_id}, 1, {retention})\n").into_bytes();

        self.client.post(&self.url).body(insert).send().await?;

        Ok(())
    }

    pub async fn optimize_table(&self) -> anyhow::Result<String> {
        let table = &self.table;
        let final_query = format!("SELECT * FROM {table} FINAL\n").into_bytes();

        let response = self.client.post(&self.url).body(final_query).send().await?;

        let update = response.text().await?;

        Ok(update)
    }

    pub async fn drop_table(&self) -> anyhow::Result<()> {
        let table = &self.table;
        let drop = format!("DROP TABLE IF EXISTS {table}\n").into_bytes();

        self.client.post(&self.url).body(drop).send().await?;

        Ok(())
    }
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
    use uuid::Uuid;

    use crate::mutations::parser::Update;

    use super::*;

    #[tokio::test]
    async fn test_simple_mutation() {
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
        // assert_eq!(batch.0[&primary_key].attr_str["a"], "b");

        let test_client = ClickhouseTestClient::new("eap_spans".to_string())
            .await
            .unwrap();

        test_client.insert_data(primary_key).await;

        let all_queries = format_query(&test_client.table, &batch);
        test_client.run_mutation(all_queries).await;

        // it is the test writer's responsibility to provide assertions
        let mutation = test_client.optimize_table().await;

        assert!(mutation.unwrap().contains("{'a':'b'}"));

        stest_client.drop_table().await;
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
