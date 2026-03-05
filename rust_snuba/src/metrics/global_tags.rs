/// Sets a tag on the current Sentry scope.
pub fn set_global_tag(key: String, value: String) {
    sentry::configure_scope(|scope| {
        scope.set_tag(&key, &value);
    });
}
