use reqwest::header::{HeaderMap, HeaderValue, ACCEPT_ENCODING, CONNECTION};
use reqwest::{Client, Error, Response};

#[derive(Clone)]
pub struct ClickhouseClient {
    client: Client,
    url: String,
    headers: HeaderMap<HeaderValue>,
    table: String,
}
impl ClickhouseClient {
    pub fn new(hostname: &str, http_port: u16, table: &str, database: &str) -> ClickhouseClient {
        let mut client = ClickhouseClient {
            client: Client::new(),
            url: format!("http://{}:{}", hostname, http_port),
            headers: HeaderMap::new(),
            table: table.to_string(),
        };

        client
            .headers
            .insert(CONNECTION, HeaderValue::from_static("keep-alive"));
        client
            .headers
            .insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip,deflate"));
        client
            .headers
            .insert("X-ClickHouse-Database", HeaderValue::from_str(database).unwrap());
        client
    }

    pub async fn send(&self, body: Vec<u8>) -> Result<Response, Error> {
        self.client
            .post(self.url.clone())
            .headers(self.headers.clone())
            .body(body)
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
    #[tokio::test]
    async fn it_works() -> Result<(), reqwest::Error> {
        let client: ClickhouseClient = ClickhouseClient::new("localhost", 8123, "querylog_local", "default");

        println!("{}", "running test");
        let res = client.send(b"[]".to_vec()).await;
        println!("Response status {}", res.unwrap().status());
        Ok(())
    }
}
