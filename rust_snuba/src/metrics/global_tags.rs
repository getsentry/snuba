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

    // GLOBAL_TAGS is a process-wide static and Rust runs tests in parallel, so each
    // test uses its own key namespace to avoid racing on shared keys.

    #[test]
    fn test_set_and_get_global_tags() {
        set_global_tag("set_get.env".to_owned(), "production".to_owned());
        set_global_tag("set_get.region".to_owned(), "us-east".to_owned());

        let tags = get_global_tags();
        assert!(tags.contains(&("set_get.env".to_owned(), "production".to_owned())));
        assert!(tags.contains(&("set_get.region".to_owned(), "us-east".to_owned())));
    }

    #[test]
    fn test_overwrite_global_tag() {
        set_global_tag("overwrite.env".to_owned(), "staging".to_owned());
        set_global_tag("overwrite.env".to_owned(), "production".to_owned());

        let tags = get_global_tags();
        let env_tags: Vec<_> = tags.iter().filter(|(k, _)| k == "overwrite.env").collect();
        assert_eq!(env_tags.len(), 1);
        assert_eq!(env_tags[0].1, "production");
    }

    #[test]
    fn test_tags_sorted_by_key() {
        set_global_tag("sorted.z_key".to_owned(), "z_val".to_owned());
        set_global_tag("sorted.a_key".to_owned(), "a_val".to_owned());

        // get_global_tags() returns entries from a BTreeMap, so keys come back sorted
        // regardless of which other tests have written to the shared static.
        let keys: Vec<_> = tags_for_prefix("sorted.");
        let mut sorted_keys = keys.clone();
        sorted_keys.sort();
        assert_eq!(keys, sorted_keys);
        assert_eq!(
            keys,
            vec!["sorted.a_key".to_owned(), "sorted.z_key".to_owned()]
        );
    }

    fn tags_for_prefix(prefix: &str) -> Vec<String> {
        get_global_tags()
            .into_iter()
            .map(|(k, _)| k)
            .filter(|k| k.starts_with(prefix))
            .collect()
    }
}
