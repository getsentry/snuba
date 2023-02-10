use reqwest::header::{HeaderMap, HeaderValue, ACCEPT_ENCODING, CONNECTION};
use reqwest::{Client, Error, Response};

pub struct ClickhouseClient {
    client: Client,
    url: String,
    headers: HeaderMap<HeaderValue>,
    table: String,
}

impl ClickhouseClient {
    pub async fn send(&self, body: String) -> Result<Response, Error> {
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

pub fn new(hostname: String, http_port: u16, table: String) -> ClickhouseClient {
    let mut client = ClickhouseClient {
        client: Client::new(),
        url: format!("http://{}:{}", hostname, http_port),
        headers: HeaderMap::new(),
        table,
    };

    client
        .headers
        .insert(CONNECTION, HeaderValue::from_static("keep-alive"));
    client
        .headers
        .insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip,deflate"));
    client
        .headers
        .insert("X-ClickHouse-Database", HeaderValue::from_static("default"));
    client
}
