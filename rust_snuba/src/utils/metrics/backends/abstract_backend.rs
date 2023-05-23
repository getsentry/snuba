use rust_arroyo::utils::metrics::MetricsClientTrait;

pub trait MetricsBackend: MetricsClientTrait {
    fn events(
        &self,
        title: &str,
        text: &str,
        alert_type: &str,
        priority: &str,
        tags: &[&str],
    );
}
