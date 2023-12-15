use anyhow::Context;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use uuid::Uuid;

use crate::types::{InsertBatch, KafkaMessageMetadata, RowData};

pub fn process_message(
    payload: KafkaPayload,
    metadata: KafkaMessageMetadata,
) -> anyhow::Result<InsertBatch> {
    let payload_bytes = payload.payload().context("Expected payload")?;

    let replay_message: ReplayMessage = serde_json::from_slice(payload_bytes)?;
    let replay_payload = serde_json::from_slice(&replay_message.payload)?;

    let output = match replay_payload {
        ReplayPayload::ClickEvent(event) => {
            let replay_rows = event.clicks.into_iter().map(|click| ReplayRow {
                click_alt: click.alt,
                click_aria_label: click.aria_label,
                click_class: click.class,
                click_id: click.id,
                click_is_dead: click.is_dead,
                click_is_rage: click.is_rage,
                click_node_id: click.node_id,
                click_role: click.role,
                click_tag: click.tag,
                click_testid: click.testid,
                click_text: click.text,
                click_title: click.title,
                event_hash: click.event_hash,
                offset: metadata.offset,
                partition: metadata.partition,
                project_id: replay_message.project_id,
                replay_id: replay_message.replay_id,
                retention_days: replay_message.retention_days,
                timestamp: click.timestamp,
                ..Default::default()
            });

            let mut rows: Vec<Vec<u8>> = Vec::with_capacity(replay_rows.len());

            for row in replay_rows {
                rows.push(serde_json::to_vec(&row)?);
            }

            rows
        }
        ReplayPayload::Event(event) => {
            let event_hash = match (event.event_hash, event.segment_id) {
                (None, None) => Uuid::new_v4(),
                (None, Some(segment_id)) => {
                    Uuid::from_slice(md5::compute(segment_id.to_string().as_bytes()).as_slice())?
                }
                (Some(event_hash), _) => event_hash,
            };

            let (ip_address_v4, ip_address_v6) = match event.user.ip_address {
                None => (None, None),
                Some(IpAddr::V4(ip)) => (Some(ip), None),
                Some(IpAddr::V6(ip)) => (None, Some(ip)),
            };

            let title = event.tags.get("title").cloned();
            let tags_key = event.tags.keys().cloned().collect::<Vec<String>>();
            let tags_value = event.tags.values().cloned().collect::<Vec<String>>();
            let user = event.user.user_id.clone();

            let replay_row = ReplayRow {
                browser_name: event.contexts.browser.name,
                browser_version: event.contexts.browser.version,
                device_brand: event.contexts.device.brand,
                device_family: event.contexts.device.family,
                device_model: event.contexts.device.model,
                device_name: event.contexts.device.name,
                dist: event.dist,
                environment: event.environment,
                error_ids: event.error_ids,
                error_sample_rate: event.contexts.replay.error_sample_rate,
                session_sample_rate: event.contexts.replay.session_sample_rate,
                event_hash,
                is_archived: event.is_archived,
                ip_address_v4,
                ip_address_v6,
                offset: metadata.offset,
                os_name: event.contexts.os.name,
                os_version: event.contexts.os.version,
                partition: metadata.partition,
                platform: event.platform,
                project_id: replay_message.project_id,
                release: event.release,
                replay_id: replay_message.replay_id,
                replay_start_timestamp: event.replay_start_timestamp,
                replay_type: event.replay_type,
                retention_days: replay_message.retention_days,
                sdk_name: event.sdk.name,
                sdk_version: event.sdk.version,
                segment_id: event.segment_id,
                timestamp: event.timestamp,
                trace_ids: event.trace_ids,
                urls: event.urls,
                user,
                user_email: event.user.email,
                user_id: event.user.user_id,
                user_name: event.user.username,
                title,
                tags_key,
                tags_value,
                ..Default::default()
            };

            vec![serde_json::to_vec(&replay_row)?]
        }
        ReplayPayload::EventLinkEvent(event) => {
            let replay_row = ReplayRow {
                debug_id: event.debug_id,
                error_id: event.error_id,
                event_hash: event.event_hash,
                fatal_id: event.fatal_id,
                info_id: event.info_id,
                offset: metadata.offset,
                partition: metadata.partition,
                project_id: replay_message.project_id,
                replay_id: replay_message.replay_id,
                retention_days: replay_message.retention_days,
                timestamp: event.timestamp,
                warning_id: event.warning_id,
                ..Default::default()
            };

            vec![serde_json::to_vec(&replay_row)?]
        }
    };

    Ok(InsertBatch {
        rows: RowData::from_rows(output),
        origin_timestamp: None,
        sentry_received_timestamp: None,
    })
}

#[derive(Debug, Deserialize)]
struct ReplayMessage {
    payload: Vec<u8>,
    project_id: u64,
    replay_id: Uuid,
    retention_days: u16,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum ReplayPayload {
    #[serde(rename = "replay_actions")]
    ClickEvent(ReplayClickEvent),
    #[serde(rename = "replay_event")]
    Event(Box<ReplayEvent>),
    #[serde(rename = "event_link")]
    EventLinkEvent(ReplayEventLinkEvent),
}

// Replay Click Event

#[derive(Debug, Deserialize)]
struct ReplayClickEvent {
    clicks: Vec<ReplayClickEventClick>,
}

#[derive(Debug, Deserialize)]
struct ReplayClickEventClick {
    alt: String,
    aria_label: String,
    class: Vec<String>,
    event_hash: Uuid,
    id: String,
    is_dead: u8,
    is_rage: u8,
    node_id: u32,
    role: String,
    tag: String,
    testid: String,
    text: String,
    timestamp: f64,
    title: String,
}

// Replay Event

#[derive(Debug, Deserialize)]
struct ReplayEvent {
    #[serde(default)]
    contexts: Contexts,
    #[serde(default)]
    dist: String,
    #[serde(default)]
    environment: String,
    #[serde(default)]
    event_hash: Option<Uuid>,
    #[serde(default)]
    is_archived: u8,
    #[serde(default = "default_platform")]
    platform: String,
    #[serde(default)]
    release: String,
    #[serde(default)]
    replay_start_timestamp: Option<f64>,
    #[serde(default)]
    replay_type: String,
    #[serde(default)]
    sdk: Version,
    segment_id: Option<u16>,
    timestamp: f64,
    #[serde(default)]
    urls: Vec<String>,
    #[serde(default)]
    user: User,
    #[serde(default)]
    trace_ids: Vec<Uuid>,
    #[serde(default)]
    error_ids: Vec<Uuid>,
    #[serde(default)]
    tags: HashMap<String, String>,
}

#[derive(Debug, Default, Deserialize)]
struct Contexts {
    #[serde(default)]
    browser: Version,
    #[serde(default)]
    device: Device,
    #[serde(default)]
    os: Version,
    #[serde(default)]
    replay: ReplayContext,
}

#[derive(Debug, Default, Deserialize)]
struct Device {
    #[serde(default)]
    brand: String,
    #[serde(default)]
    family: String,
    #[serde(default)]
    model: String,
    #[serde(default)]
    name: String,
}

#[derive(Debug, Default, Deserialize)]
struct ReplayContext {
    #[serde(default = "default_sample")]
    error_sample_rate: f64,
    #[serde(default = "default_sample")]
    session_sample_rate: f64,
}

#[derive(Debug, Default, Deserialize)]
struct User {
    #[serde(default)]
    username: String,
    #[serde(default)]
    user_id: String,
    #[serde(default)]
    email: String,
    #[serde(default)]
    ip_address: Option<IpAddr>,
}

#[derive(Debug, Default, Deserialize)]
struct Version {
    #[serde(default)]
    name: String,
    #[serde(default)]
    version: String,
}

fn default_platform() -> String {
    "javascript".to_string()
}

fn default_sample() -> f64 {
    -1.0
}

// ReplayEventLink

#[derive(Debug, Deserialize)]
struct ReplayEventLinkEvent {
    timestamp: f64,
    event_hash: Uuid,
    #[serde(default)]
    debug_id: Uuid,
    #[serde(default)]
    error_id: Uuid,
    #[serde(default)]
    fatal_id: Uuid,
    #[serde(default)]
    info_id: Uuid,
    #[serde(default)]
    warning_id: Uuid,
}

// ReplayRow is not an exact match with the schema. We're trying to remove many of the nullable
// columns. These nullable columns are written to with empty values and will eventually have
// their null condition dropped.
#[derive(Debug, Default, Serialize)]
struct ReplayRow {
    replay_id: Uuid,
    debug_id: Uuid,
    info_id: Uuid,
    warning_id: Uuid,
    error_id: Uuid,
    fatal_id: Uuid,
    replay_type: String,
    error_sample_rate: f64,
    session_sample_rate: f64,
    event_hash: Uuid,
    segment_id: Option<u16>,
    trace_ids: Vec<Uuid>,
    title: Option<String>,
    url: String,
    urls: Vec<String>,
    is_archived: u8,
    error_ids: Vec<Uuid>,
    project_id: u64,
    timestamp: f64,
    replay_start_timestamp: Option<f64>,
    platform: String,
    environment: String,
    release: String,
    dist: String,
    ip_address_v4: Option<Ipv4Addr>,
    ip_address_v6: Option<Ipv6Addr>,
    user: String,
    user_id: String,
    user_name: String,
    user_email: String,
    os_name: String,
    os_version: String,
    browser_name: String,
    browser_version: String,
    device_name: String,
    device_brand: String,
    device_family: String,
    device_model: String,
    sdk_name: String,
    sdk_version: String,
    #[serde(rename = "tags.key")]
    tags_key: Vec<String>,
    #[serde(rename = "tags.value")]
    tags_value: Vec<String>,
    click_node_id: u32,
    click_tag: String,
    click_id: String,
    click_class: Vec<String>,
    click_text: String,
    click_role: String,
    click_alt: String,
    click_testid: String,
    click_aria_label: String,
    click_title: String,
    click_is_dead: u8,
    click_is_rage: u8,
    retention_days: u16,
    partition: u16,
    offset: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;
    use rust_arroyo::backends::kafka::types::KafkaPayload;
    use std::time::SystemTime;

    #[test]
    fn test_parse_replay_event() {
        let payload = r#"{
            "contexts": {
                "browser": {
                    "name": "browser",
                    "verison": "v1"
                },
                "device": {
                    "brand": "brand",
                    "family": "family",
                    "model": "model",
                    "name": "name"
                },
                "os": {
                    "name": "os",
                    "verison": "v1"
                },
                "replay": {
                    "error_sample_rate": 1,
                    "session_sample_rate": 0.5
                }
            },
            "user": {
                "email": "email",
                "ip_address": "127.0.0.1",
                "user_id": "user_id",
                "username": "username"
            },
            "sdk": {
                "name": "sdk",
                "verison": "v1"
            },
            "dist": "dist",
            "environment": "environment",
            "is_archived": 0,
            "platform": "platform",
            "release": "release",
            "replay_start_timestamp": 1702659277,
            "replay_type": "buffer",
            "urls": ["urls"],
            "trace_ids": ["2cd798d70f9346089026d2014a826629"],
            "error_ids": ["df11e6d952da470386a64340f13151c4"],
            "tags": {"a": "b", "transaction.name": "test"},
            "segment_id": 0,
            "replay_id": "048aa04be40243948eb3b57089c519ee",
            "timestamp": 1702659277,
            "type": "replay_event"
        }"#;
        let payload_value = payload.as_bytes();

        let data = format!(
            r#"{{
                "payload": {payload_value:?},
                "project_id": 1,
                "replay_id": "048aa04be40243948eb3b57089c519ee",
                "retention_days": 30,
                "segment_id": null,
                "start_time": 100,
                "type": "replay_event"
            }}"#
        );
        let payload = KafkaPayload::new(None, None, Some(data.as_bytes().to_vec()));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };
        process_message(payload, meta).expect("The message should be processed");
    }

    #[test]
    fn test_parse_replay_click_event() {
        let payload = r#"{
            "type": "replay_actions",
        "clicks": [{
            "alt": "Alternate",
            "aria_label": "Aria-label",
            "class": ["hello", "world"],
            "event_hash": "b4370ef8d1994e96b5bc719b72afbf49",
            "id": "id",
            "is_dead": 0,
            "is_rage": 1,
            "node_id": 320,
            "replay_id": "048aa04be40243948eb3b57089c519ee",
            "role": "button",
            "tag": "div",
            "testid": "",
            "text": "Submit",
            "timestamp": 1702659277,
            "title": "title"
        }]
        }"#;
        let payload_value = payload.as_bytes();

        let data = format!(
            r#"{{
                "payload": {payload_value:?},
                "project_id": 1,
                "replay_id": "048aa04be40243948eb3b57089c519ee",
                "retention_days": 30,
                "segment_id": null,
                "start_time": 100,
                "type": "replay_event"
            }}"#
        );
        let payload = KafkaPayload::new(None, None, Some(data.as_bytes().to_vec()));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };
        process_message(payload, meta).expect("The message should be processed");
    }

    #[test]
    fn test_parse_replay_event_link_event() {
        let payload = r#"{
            "debug_id": "5d51f0eb1d1244e7a8312d8a248cd987",
            "event_hash": "b4370ef8d1994e96b5bc719b72afbf49",
            "replay_id": "048aa04be40243948eb3b57089c519ee",
            "timestamp": 1702659277,
            "type": "event_link"
        }"#;
        let payload_value = payload.as_bytes();

        let data = format!(
            r#"{{
                "payload": {payload_value:?},
                "project_id": 1,
                "replay_id": "048aa04be40243948eb3b57089c519ee",
                "retention_days": 30,
                "segment_id": null,
                "start_time": 100,
                "type": "replay_event"
            }}"#
        );
        let payload = KafkaPayload::new(None, None, Some(data.as_bytes().to_vec()));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };
        process_message(payload, meta).expect("The message should be processed");
    }

    #[test]
    fn test_parse_replay_event_sparse() {
        let payload = r#"{
            "contexts": {
                "browser": {
                    "name": "browser",
                    "verison": "v1"
                },
                "device": {
                    "brand": "brand",
                    "family": "family",
                    "model": "model",
                    "name": "name"
                },
                "os": {
                    "name": "os",
                    "verison": "v1"
                },
                "replay": {
                    "error_sample_rate": 1,
                    "session_sample_rate": 0.5
                }
            },
            "user": {
                "email": "email",
                "ip_address": "127.0.0.1",
                "user_id": "user_id",
                "username": "username"
            },
            "sdk": {
                "name": "sdk",
                "verison": "v1"
            },
            "dist": "dist",
            "environment": "environment",
            "is_archived": 0,
            "platform": "platform",
            "release": "release",
            "replay_start_timestamp": 1702659277,
            "replay_type": "buffer",
            "urls": ["urls"],
            "trace_ids": ["2cd798d70f9346089026d2014a826629"],
            "error_ids": ["df11e6d952da470386a64340f13151c4"],
            "tags": {"a": "b", "transaction.name": "test"},
            "segment_id": 0,
            "replay_id": "048aa04be40243948eb3b57089c519ee",
            "timestamp": 1702659277,
            "type": "replay_event"
        }"#;
        let payload_value = payload.as_bytes();

        let data = format!(
            r#"{{
                "payload": {payload_value:?},
                "project_id": 1,
                "replay_id": "048aa04be40243948eb3b57089c519ee",
                "retention_days": 30,
                "segment_id": null,
                "start_time": 100,
                "type": "replay_event"
            }}"#
        );
        let payload = KafkaPayload::new(None, None, Some(data.as_bytes().to_vec()));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };
        process_message(payload, meta).expect("The message should be processed");
    }
}
