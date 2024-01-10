use std::collections::BTreeMap;

use parking_lot::RwLock;
use statsdproxy::middleware::Middleware;
use statsdproxy::types::Metric;

static GLOBAL_TAGS: RwLock<BTreeMap<String, String>> = RwLock::new(BTreeMap::new());

pub fn set_global_tag(key: String, value: String) {
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
    pub fn new_with_tagmap(next: M, global_tags: &'a RwLock<BTreeMap<String, String>>) -> Self {
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