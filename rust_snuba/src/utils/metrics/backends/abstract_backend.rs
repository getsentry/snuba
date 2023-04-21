use rust_arroyo::utils::metrics::MetricsClientTrait;

pub trait MetricsBackend: MetricsClientTrait {
    // fn increment(&self, name: &str, value: i64, tags: &[&str]);
    // fn gauge(&self, name: &str, value: f64, tags: &[&str]);
    // fn timing(&self, name: &str, value: i64, tags: &[&str]);
    fn events(
        &self,
        title: &str,
        text: &str,
        alert_type: &str,
        priority: &str,
        tags: &[&str],
    );
    // fn counter(&self, name: &str, value: f64, tags: &[&str]) -> Result<(), Error>;
    // fn histogram(&self, name: &str, value: f64, tags: &[&str]) -> Result<(), Error>;
}
