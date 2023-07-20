use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct BytesInsertBatch {
    pub rows: Vec<Vec<u8>>,
}
