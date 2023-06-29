use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderValue, ACCEPT_ENCODING, CONNECTION};
use reqwest::{Client, Error, Response};

pub struct ClickhouseClient {
    client: Client,
    url: String,
    headers: HeaderMap<HeaderValue>,
    table: String,
}
impl ClickhouseClient {
    pub fn new(hostname: &str, http_port: u16, table: &str, database: Option<&str>) -> ClickhouseClient {
        let mut client = ClickhouseClient {
            client: Client::new(),
            url: format!("http://{}:{}", hostname, http_port),
            headers: HeaderMap::new(),
            table: table.to_string(),
        };

        let database_name = database.unwrap_or("default");
        client
            .headers
            .insert(CONNECTION, HeaderValue::from_static("keep-alive"));
        client
            .headers
            .insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip,deflate"));
        client
            .headers
            .insert("X-ClickHouse-Database", HeaderValue::from_str(database_name).unwrap());
        client
    }

    pub async fn send(&self, body: String) -> Result<Response, Error> {
        self.client
            .post(self.url.clone())
            .headers(self.headers.clone())
            .body(body)
            .timeout(Duration::from_secs(30))
            .query(&[(
                "query",
                format!("INSERT INTO {} FORMAT JSONEachRow", self.table),
            )])
            .send()
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[ignore = "clickhouse not running in rust ci"]
    #[tokio::test]
    async fn it_works() -> Result<(), reqwest::Error> {
        let client: ClickhouseClient = ClickhouseClient::new("localhost", 8123, "querylog_local", None);
        println!("{}", "running test");
        let res = client.send("[]".to_string()).await;
        println!("Response status {}", res.unwrap().status());
        Ok(())
    }

    #[ignore = "clickhouse not running in rust ci"]
    #[tokio::test]
    async fn select() -> Result<(), reqwest::Error> {
        let client: ClickhouseClient = ClickhouseClient::new("localhost", 8123, "querylog_local", None);
        client.client
            .get(client.url.clone())
            .headers(client.headers.clone())
            .query(&[(
                "query",
                "SELECT 1"
            )])
            .send().await?;
        Ok(())
    }
}
