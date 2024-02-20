use std::collections::BTreeMap;

use parking_lot::RwLock;
use statsdproxy::middleware::Middleware;
use statsdproxy::types::Metric;

static GLOBAL_TAGS: RwLock<BTreeMap<String, String>> = RwLock::new(BTreeMap::new());

pub fn set_global_tag(key: String, value: String) {
    sentry::configure_scope(|scope| {
        scope.set_tag(&key, &value);
    });
    GLOBAL_TAGS.write().insert(key, value);
}

pub struct AddGlobalTags<'a, M> {
    next: M,
    global_tags: &'a RwLock<BTreeMap<String, String>>,
}

impl<M> AddGlobalTags<'static, M>
where
    M: Middleware,
{
    pub fn new(next: M) -> Self {
        Self::new_with_tagmap(next, &GLOBAL_TAGS)
    }
}

impl<'a, M> AddGlobalTags<'a, M>
where
    M: Middleware,
{
    fn new_with_tagmap(next: M, global_tags: &'a RwLock<BTreeMap<String, String>>) -> Self {
        AddGlobalTags { next, global_tags }
    }
}

impl<'a, M> Middleware for AddGlobalTags<'a, M>
where
    M: Middleware,
{
    fn poll(&mut self) {
        self.next.poll()
    }

    fn submit(&mut self, metric: &mut Metric) {
        let global_tags = self.global_tags.read();

        if global_tags.is_empty() {
            return self.next.submit(metric);
        }

        let mut tag_buffer: Vec<u8> = Vec::new();
        let mut add_comma = false;
        match metric.tags() {
            Some(tags) if !tags.is_empty() => {
                tag_buffer.extend(tags);
                add_comma = true;
            }
            _ => (),
        }
        for (k, v) in global_tags.iter() {
            if add_comma {
                tag_buffer.push(b',');
            }
            tag_buffer.extend(k.as_bytes());
            tag_buffer.push(b':');
            tag_buffer.extend(v.as_bytes());
            add_comma = true;
        }

        metric.set_tags(&tag_buffer);

        self.next.submit(metric)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cell::RefCell;
    use std::collections::BTreeMap;

    use statsdproxy::{middleware::Middleware, types::Metric};

    struct FnStep<F>(pub F);

    impl<F> Middleware for FnStep<F>
    where
        F: FnMut(&mut Metric),
    {
        fn submit(&mut self, metric: &mut Metric) {
            (self.0)(metric)
        }
    }

    #[test]
    fn test_basic() {
        let test_cases = [
            // Without tags
            ("users.online:1|c", "users.online:1|c|#env:prod"),
            // With tags
            (
                "users.online:1|c|#tag1:a",
                "users.online:1|c|#tag1:a,env:prod",
            ),
        ];

        for test_case in test_cases {
            let results = RefCell::new(vec![]);
            let global_tags = RwLock::new(BTreeMap::from([("env".to_owned(), "prod".to_owned())]));

            let step = FnStep(|metric: &mut Metric| results.borrow_mut().push(metric.clone()));
            let mut middleware = AddGlobalTags::new_with_tagmap(step, &global_tags);

            let mut metric = Metric::new(test_case.0.as_bytes().to_vec());
            middleware.submit(&mut metric);
            assert_eq!(results.borrow().len(), 1);
            let updated_metric = Metric::new(results.borrow_mut()[0].raw.clone());
            assert_eq!(updated_metric.raw, test_case.1.as_bytes());
        }
    }
}
