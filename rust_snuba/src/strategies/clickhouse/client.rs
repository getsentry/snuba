use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
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

/// Trait for typed rows that can be inserted into ClickHouse using the native client.
/// This allows storing typed rows in a type-erased manner while still being able to
/// insert them using the native RowBinary format.
#[async_trait]
pub trait InsertableRows: Send + Sync + std::fmt::Debug {
    /// Insert these rows into ClickHouse using the provided client.
    async fn insert_into(
        &self,
        client: &NativeClickhouseClient,
        retry_config: RetryConfig,
    ) -> anyhow::Result<()>;

    /// Get the number of rows.
    fn len(&self) -> usize;

    /// Check if there are no rows.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return self as Any for downcasting.
    fn as_any(&self) -> &dyn Any;

    /// Try to merge another InsertableRows into this one, returning a new merged instance.
    /// Returns None if the types don't match or merging is not supported.
    /// The default implementation returns None (no merging support).
    fn try_merge(&self, other: &dyn InsertableRows) -> Option<Arc<dyn InsertableRows>> {
        let _ = other;
        None
    }
}

/// A type-erased container for rows that can be inserted via the native client.
/// This wraps `Vec<T>` where T implements the required traits for ClickHouse insertion.
#[derive(Debug)]
pub struct TypedRows<T> {
    rows: Vec<T>,
}

impl<T> TypedRows<T> {
    pub fn new(rows: Vec<T>) -> Self {
        Self { rows }
    }

    #[allow(dead_code)]
    pub fn into_inner(self) -> Vec<T> {
        self.rows
    }
}

#[async_trait]
impl<T> InsertableRows for TypedRows<T>
where
    T: clickhouse::Row + serde::Serialize + Send + Sync + std::fmt::Debug + Clone + 'static,
{
    async fn insert_into(
        &self,
        client: &NativeClickhouseClient,
        retry_config: RetryConfig,
    ) -> anyhow::Result<()> {
        client.insert_rows(&self.rows, retry_config).await
    }

    fn len(&self) -> usize {
        self.rows.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn try_merge(&self, other: &dyn InsertableRows) -> Option<Arc<dyn InsertableRows>> {
        // Try to downcast the other to the same TypedRows<T> type
        let other_any = other.as_any();
        if let Some(other_typed) = other_any.downcast_ref::<TypedRows<T>>() {
            // Clone both row vectors and concatenate them
            let mut merged_rows = self.rows.clone();
            merged_rows.extend(other_typed.rows.iter().cloned());
            Some(Arc::new(TypedRows::new(merged_rows)))
        } else {
            None
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
    pub async fn insert_rows<T>(&self, rows: &[T], retry_config: RetryConfig) -> anyhow::Result<()>
    where
        T: clickhouse::Row + serde::Serialize + Send,
    {
        for attempt in 0..=retry_config.max_retries {
            let result = self.try_insert(rows).await;

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
            use_native_client: true,
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
            use_native_client: true,
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

    // Test row type for type-aware merge tests
    #[derive(Debug, Clone, serde::Serialize, clickhouse::Row)]
    struct TestRow {
        id: u64,
        name: String,
    }

    #[derive(Debug, Clone, serde::Serialize, clickhouse::Row)]
    struct DifferentRow {
        value: i32,
    }

    #[test]
    fn test_typed_rows_merge_same_type() {
        let rows1: Arc<dyn InsertableRows> = Arc::new(TypedRows::new(vec![
            TestRow {
                id: 1,
                name: "a".to_string(),
            },
            TestRow {
                id: 2,
                name: "b".to_string(),
            },
        ]));

        let rows2: Arc<dyn InsertableRows> = Arc::new(TypedRows::new(vec![TestRow {
            id: 3,
            name: "c".to_string(),
        }]));

        // Should successfully merge same types
        let merged = rows1.try_merge(rows2.as_ref());
        assert!(merged.is_some());

        let merged = merged.unwrap();
        assert_eq!(merged.len(), 3);
    }

    #[test]
    fn test_typed_rows_merge_different_types() {
        let rows1: Arc<dyn InsertableRows> = Arc::new(TypedRows::new(vec![TestRow {
            id: 1,
            name: "a".to_string(),
        }]));

        let rows2: Arc<dyn InsertableRows> =
            Arc::new(TypedRows::new(vec![DifferentRow { value: 42 }]));

        // Should fail to merge different types
        let merged = rows1.try_merge(rows2.as_ref());
        assert!(merged.is_none());
    }

    #[test]
    fn test_typed_rows_len() {
        let rows = TypedRows::new(vec![
            TestRow {
                id: 1,
                name: "a".to_string(),
            },
            TestRow {
                id: 2,
                name: "b".to_string(),
            },
        ]);

        assert_eq!(rows.len(), 2);
        assert!(!rows.is_empty());

        let empty_rows: TypedRows<TestRow> = TypedRows::new(vec![]);
        assert_eq!(empty_rows.len(), 0);
        assert!(empty_rows.is_empty());
    }
}
