use crate::config::EnvConfig;
use anyhow::Context;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use uuid::Uuid;

use crate::types::{InsertBatch, KafkaMessageMetadata};

pub fn process_message(
    payload: KafkaPayload,
    metadata: KafkaMessageMetadata,
    _config: &EnvConfig,
) -> anyhow::Result<InsertBatch> {
    let payload_bytes = payload.payload().context("Expected payload")?;

    let replay_message: ReplayMessage = serde_json::from_slice(payload_bytes)?;
    let replay_payload = serde_json::from_slice(&replay_message.payload)?;

    match replay_payload {
        ReplayPayload::ClickEvent(event) => {
            InsertBatch::from_rows(event.clicks.into_iter().map(|click| ReplayRow {
                click_alt: click.alt,
                click_aria_label: click.aria_label,
                click_class: click.class,
                click_id: click.id,
                click_is_dead: click.is_dead,
                click_is_rage: click.is_rage,
                click_node_id: click.node_id,
                click_component_name: click.component_name,
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
                timestamp: click.timestamp as u32,
                ..Default::default()
            }))
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

            // Tags normalization and title extraction.
            let mut title = None;
            let mut tags_key = Vec::with_capacity(event.tags.len());
            let mut tags_value = Vec::with_capacity(event.tags.len());

            for tag in event.tags.into_iter() {
                if &tag.0 == "transaction" {
                    title = Some(tag.1.clone())
                }
                tags_key.push(tag.0);
                tags_value.push(tag.1);
            }

            // Compute user value.
            let user = if !event.user.user_id.is_empty() {
                event.user.user_id.clone()
            } else if !event.user.username.is_empty() {
                event.user.username.clone()
            } else if !event.user.email.is_empty() {
                event.user.email.clone()
            } else if let Some(ip) = event.user.ip_address {
                ip.to_string()
            } else {
                String::new()
            };

            // Sample-rate normalization. Null values are inserted as a -1.0 sentinel value.
            let error_sample_rate = event.contexts.replay.error_sample_rate.unwrap_or(-1.0);
            let session_sample_rate = event.contexts.replay.session_sample_rate.unwrap_or(-1.0);

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
                error_sample_rate,
                session_sample_rate,
                event_hash,
                is_archived: event.is_archived.into(),
                ip_address_v4,
                ip_address_v6,
                offset: metadata.offset,
                os_name: event.contexts.os.name,
                os_version: event.contexts.os.version,
                partition: metadata.partition,
                platform: event.platform,
                project_id: replay_message.project_id,
                release: event.release,
                replay_id: event.replay_id,
                replay_start_timestamp: event.replay_start_timestamp.map(|v| v as u32),
                replay_type: event.replay_type,
                retention_days: replay_message.retention_days,
                sdk_name: event.sdk.name,
                sdk_version: event.sdk.version,
                segment_id: event.segment_id,
                timestamp: event.timestamp as u32,
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

            InsertBatch::from_rows([replay_row])
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
                timestamp: event.timestamp as u32,
                warning_id: event.warning_id,
                ..Default::default()
            };

            InsertBatch::from_rows([replay_row])
        }
    }
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
    #[serde(default)]
    component_name: String,
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
    replay_id: Uuid,
    #[serde(default)]
    contexts: Contexts,
    #[serde(default)]
    dist: String,
    #[serde(default)]
    environment: String,
    #[serde(default)]
    event_hash: Option<Uuid>,
    #[serde(default)]
    is_archived: bool,
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
    tags: Vec<(String, String)>,
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
    #[serde(default)]
    error_sample_rate: Option<f64>,
    #[serde(default)]
    session_sample_rate: Option<f64>,
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
    #[serde(skip_serializing_if = "uuid::Uuid::is_nil")]
    debug_id: Uuid,
    #[serde(skip_serializing_if = "uuid::Uuid::is_nil")]
    info_id: Uuid,
    #[serde(skip_serializing_if = "uuid::Uuid::is_nil")]
    warning_id: Uuid,
    #[serde(skip_serializing_if = "uuid::Uuid::is_nil")]
    error_id: Uuid,
    #[serde(skip_serializing_if = "uuid::Uuid::is_nil")]
    fatal_id: Uuid,
    replay_type: String,
    error_sample_rate: f64,
    session_sample_rate: f64,
    event_hash: Uuid,
    segment_id: Option<u16>,
    trace_ids: Vec<Uuid>,
    title: Option<String>,
    urls: Vec<String>,
    is_archived: u8,
    error_ids: Vec<Uuid>,
    project_id: u64,
    timestamp: u32,
    replay_start_timestamp: Option<u32>,
    platform: String,
    environment: String,
    release: String,
    dist: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    ip_address_v4: Option<Ipv4Addr>,
    #[serde(skip_serializing_if = "Option::is_none")]
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
    #[serde(skip_serializing_if = "is_u32_zero")]
    click_node_id: u32,
    #[serde(skip_serializing_if = "String::is_empty")]
    click_tag: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    click_id: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    click_class: Vec<String>,
    #[serde(skip_serializing_if = "String::is_empty")]
    click_text: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    click_role: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    click_alt: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    click_testid: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    click_aria_label: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    click_title: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    click_component_name: String,
    #[serde(skip_serializing_if = "is_u8_zero")]
    click_is_dead: u8,
    #[serde(skip_serializing_if = "is_u8_zero")]
    click_is_rage: u8,
    retention_days: u16,
    partition: u16,
    offset: u64,
}

fn is_u8_zero(v: &u8) -> bool {
    *v == 0
}

fn is_u32_zero(v: &u32) -> bool {
    *v == 0
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
            "is_archived": false,
            "platform": "platform",
            "release": "release",
            "replay_start_timestamp": 1702659277,
            "replay_type": "buffer",
            "urls": ["urls"],
            "trace_ids": ["2cd798d70f9346089026d2014a826629"],
            "error_ids": ["df11e6d952da470386a64340f13151c4"],
            "tags": [
                ["a", "b"],
                ["transaction.name", "test"]
            ],
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
        process_message(payload, meta, &EnvConfig::default())
            .expect("The message should be processed");
    }

    #[test]
    fn test_parse_replay_click_event() {
        let payload = r#"{
            "type": "replay_actions",
            "replay_id": "048aa04be40243948eb3b57089c519ee",
            "clicks": [{
                "alt": "Alternate",
                "aria_label": "Aria-label",
                "class": ["hello", "world"],
                "event_hash": "b4370ef8d1994e96b5bc719b72afbf49",
                "id": "id",
                "is_dead": 0,
                "is_rage": 1,
                "node_id": 320,
                "component_name": "SignUpButton",
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
        process_message(payload, meta, &EnvConfig::default())
            .expect("The message should be processed");
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
        process_message(payload, meta, &EnvConfig::default())
            .expect("The message should be processed");
    }

    #[test]
    fn test_parse_replay_event_sparse() {
        let payload = r#"{
            "is_archived": true,
            "timestamp": 1702659277,
            "type": "replay_event",
            "replay_id": "048aa04be40243948eb3b57089c519ee"
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
        process_message(payload, meta, &EnvConfig::default())
            .expect("The message should be processed");
    }
}
