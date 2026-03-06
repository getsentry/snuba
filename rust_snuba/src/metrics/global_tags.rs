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
