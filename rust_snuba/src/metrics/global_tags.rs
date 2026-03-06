use std::collections::BTreeMap;
use std::sync::{LazyLock, RwLock};

static GLOBAL_TAGS: LazyLock<RwLock<BTreeMap<String, String>>> =
    LazyLock::new(|| RwLock::new(BTreeMap::new()));

/// Sets a tag on the current Sentry scope and stores it for DogStatsD metrics.
pub fn set_global_tag(key: String, value: String) {
    sentry::configure_scope(|scope| {
        scope.set_tag(&key, &value);
    });
    GLOBAL_TAGS.write().unwrap().insert(key, value);
}

pub fn get_global_tags() -> Vec<(String, String)> {
    GLOBAL_TAGS
        .read()
        .unwrap()
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_and_get_global_tags() {
        set_global_tag("env".to_owned(), "production".to_owned());
        set_global_tag("region".to_owned(), "us-east".to_owned());

        let tags = get_global_tags();
        assert!(tags.contains(&("env".to_owned(), "production".to_owned())));
        assert!(tags.contains(&("region".to_owned(), "us-east".to_owned())));
    }

    #[test]
    fn test_overwrite_global_tag() {
        set_global_tag("env".to_owned(), "staging".to_owned());
        set_global_tag("env".to_owned(), "production".to_owned());

        let tags = get_global_tags();
        let env_tags: Vec<_> = tags.iter().filter(|(k, _)| k == "env").collect();
        assert_eq!(env_tags.len(), 1);
        assert_eq!(env_tags[0].1, "production");
    }

    #[test]
    fn test_tags_sorted_by_key() {
        set_global_tag("z_key".to_owned(), "z_val".to_owned());
        set_global_tag("a_key".to_owned(), "a_val".to_owned());

        let tags = get_global_tags();
        let keys: Vec<_> = tags.iter().map(|(k, _)| k.clone()).collect();
        let mut sorted_keys = keys.clone();
        sorted_keys.sort();
        assert_eq!(keys, sorted_keys);
    }
}
