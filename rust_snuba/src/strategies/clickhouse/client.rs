use std::time::Duration;

use clickhouse::Client;
use sentry_arroyo::counter;

use crate::config::ClickhouseConfig;

/// Configuration for retry behavior when writing to ClickHouse.
#[derive(Clone)]
pub struct RetryConfig {
    pub initial_backoff_ms: f64,
    pub max_retries: usize,
    pub jitter_factor: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            initial_backoff_ms: 500.0,
            max_retries: 4,
            jitter_factor: 0.2,
        }
    }
}

/// A wrapper around the official ClickHouse client that handles connection
/// configuration and provides a consistent interface for inserts.
#[derive(Clone)]
pub struct NativeClickhouseClient {
    client: Client,
    table: String,
}

impl NativeClickhouseClient {
    /// Create a new ClickHouse client from the given configuration.
    pub fn new(config: &ClickhouseConfig, table: &str) -> Self {
        let scheme = if config.secure { "https" } else { "http" };
        let url = format!("{}://{}:{}", scheme, config.host, config.http_port);

        let client = Client::default()
            .with_url(&url)
            .with_user(&config.user)
            .with_password(&config.password)
            .with_database(&config.database)
            .with_option("insert_distributed_sync", "1")
            .with_option("load_balancing", "in_order");

        Self {
            client,
            table: table.to_string(),
        }
    }

    /// Get a reference to the underlying clickhouse client.
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Get the table name this client is configured to write to.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Insert rows into ClickHouse using the native RowBinary format.
    /// This method handles retries with exponential backoff.
    pub async fn insert<T>(&self, rows: Vec<T>, retry_config: RetryConfig) -> anyhow::Result<()>
    where
        T: clickhouse::Row + serde::Serialize + Send,
    {
        for attempt in 0..=retry_config.max_retries {
            let result = self.try_insert(&rows).await;

            match result {
                Ok(()) => return Ok(()),
                Err(e) => {
                    let error_string = e.to_string();
                    if attempt == retry_config.max_retries {
                        counter!("rust_consumer.clickhouse_insert_error", 1, "status" => "error", "retried" => "false");
                        anyhow::bail!(
                            "error writing to clickhouse after {} attempts: {}",
                            retry_config.max_retries + 1,
                            error_string
                        );
                    }

                    counter!("rust_consumer.clickhouse_insert_error", 1, "status" => "error", "retried" => "true");
                    tracing::warn!(
                        "ClickHouse write failed (attempt {}/{}): {}",
                        attempt + 1,
                        retry_config.max_retries + 1,
                        error_string
                    );

                    // Calculate exponential backoff delay
                    let backoff_ms =
                        retry_config.initial_backoff_ms * (2_u64.pow(attempt as u32) as f64);
                    let jitter = rand::random::<f64>() * retry_config.jitter_factor
                        - retry_config.jitter_factor / 2.0;
                    let delay = Duration::from_millis((backoff_ms * (1.0 + jitter)).round() as u64);

                    tracing::debug!(
                        "Retrying in {:?} (attempt {}/{})",
                        delay,
                        attempt + 1,
                        retry_config.max_retries
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }

        unreachable!("Loop should always return or bail before reaching here");
    }

    /// Attempt a single insert operation.
    async fn try_insert<T>(&self, rows: &[T]) -> Result<(), clickhouse::error::Error>
    where
        T: clickhouse::Row + serde::Serialize + Send,
    {
        let mut insert = self.client.insert(&self.table)?;
        for row in rows {
            insert.write(row).await?;
        }
        insert.end().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let config = ClickhouseConfig {
            host: "127.0.0.1".to_string(),
            port: 9000,
            secure: false,
            http_port: 8123,
            user: "default".to_string(),
            password: "".to_string(),
            database: "default".to_string(),
        };

        let client = NativeClickhouseClient::new(&config, "test_table");
        assert_eq!(client.table(), "test_table");
    }

    #[test]
    fn test_client_creation_secure() {
        let config = ClickhouseConfig {
            host: "clickhouse.example.com".to_string(),
            port: 9440,
            secure: true,
            http_port: 8443,
            user: "admin".to_string(),
            password: "secret".to_string(),
            database: "production".to_string(),
        };

        let client = NativeClickhouseClient::new(&config, "events");
        assert_eq!(client.table(), "events");
    }

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.initial_backoff_ms, 500.0);
        assert_eq!(config.max_retries, 4);
        assert_eq!(config.jitter_factor, 0.2);
    }
}
