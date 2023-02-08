pub mod backends;
pub mod processing;
pub mod types;
pub mod utils;
pub mod storage;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
