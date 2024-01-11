use crate::config::ProcessorConfig;
use anyhow::Context;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use uuid::Uuid;

use crate::types::{InsertBatch, KafkaMessageMetadata};

pub fn process_message(
    payload: KafkaPayload,
    metadata: KafkaMessageMetadata,
    _config: &ProcessorConfig,
) -> anyhow::Result<InsertBatch> {
    let payload_bytes = payload.payload().context("Expected payload")?;
    let replay_row = deserialize_message(payload_bytes, metadata.partition, metadata.offset)?;
    InsertBatch::from_rows(replay_row)
}

pub fn deserialize_message(
    payload: &[u8],
    partition: u16,
    offset: u64,
) -> anyhow::Result<Vec<ReplayRow>> {
    let replay_message: ReplayMessage = serde_json::from_slice(payload)?;
    let replay_payload = serde_json::from_slice(&replay_message.payload)?;

    match replay_payload {
        ReplayPayload::ClickEvent(event) => Ok(event
            .clicks
            .into_iter()
            .map(|click| ReplayRow {
                click_alt: click.alt,
                click_aria_label: click.aria_label,
                click_class: click.class,
                click_component_name: click.component_name,
                click_id: click.id,
                click_is_dead: click.is_dead,
                click_is_rage: click.is_rage,
                click_node_id: click.node_id,
                click_role: click.role,
                click_tag: click.tag,
                click_testid: click.testid,
                click_text: click.text,
                click_title: click.title,
                error_sample_rate: -1.0,
                event_hash: click.event_hash,
                offset,
                partition,
                platform: "javascript".to_string(),
                project_id: replay_message.project_id,
                replay_id: replay_message.replay_id,
                retention_days: replay_message.retention_days,
                session_sample_rate: -1.0,
                timestamp: click.timestamp as u32,
                ..Default::default()
            })
            .collect()),
        ReplayPayload::Event(event) => {
            let event_hash = match (event.event_hash, event.segment_id) {
                (None, None) => Uuid::new_v4(),
                (None, Some(segment_id)) => {
                    Uuid::from_slice(md5::compute(segment_id.to_string().as_bytes()).as_slice())?
                }
                (Some(event_hash), _) => event_hash,
            };

            // Tags normalization and title extraction.
            let mut title = None;
            let mut tags_key = Vec::with_capacity(event.tags.len());
            let mut tags_value = Vec::with_capacity(event.tags.len());

            for tag in event.tags.into_iter() {
                if &tag.0 == "transaction" {
                    title = tag.1.clone()
                }
                tags_key.push(tag.0);
                tags_value.push(tag.1);
            }

            // Unwrap the ip-address string.
            let ip_address_string = event.user.ip_address.unwrap_or_default();
            let (ip_address_v4, ip_address_v6) = match ip_address_string.parse::<IpAddr>() {
                Err(_) => (None, None),
                Ok(IpAddr::V4(ipv4)) => (Some(ipv4), None),
                Ok(IpAddr::V6(ipv6)) => (None, Some(ipv6)),
            };

            // Handle user-id field.
            let user_id = match &event.user.user_id {
                Some(UserId::String(v)) => Some(v.clone()),
                Some(UserId::Number(v)) => Some(v.to_string()),
                None => None,
            };

            // Handle user field.
            let user = user_id
                .clone()
                .or(event.user.username.clone())
                .or(event.user.email.clone())
                .or(ip_address_v4.as_ref().map(|v| v.to_string()))
                .or(ip_address_v6.as_ref().map(|v| v.to_string()))
                .unwrap_or_default();

            // Sample-rate normalization. Null values are inserted as a -1.0 sentinel value.
            let error_sample_rate = event.contexts.replay.error_sample_rate.unwrap_or(-1.0);
            let session_sample_rate = event.contexts.replay.session_sample_rate.unwrap_or(-1.0);

            Ok(vec![ReplayRow {
                browser_name: event.contexts.browser.name.unwrap_or_default(),
                browser_version: event.contexts.browser.version.unwrap_or_default(),
                device_brand: event.contexts.device.brand.unwrap_or_default(),
                device_family: event.contexts.device.family.unwrap_or_default(),
                device_model: event.contexts.device.model.unwrap_or_default(),
                device_name: event.contexts.device.name.unwrap_or_default(),
                dist: event.dist.unwrap_or_default(),
                environment: event.environment.unwrap_or_default(),
                error_ids: event.error_ids.unwrap_or_default(),
                error_sample_rate,
                session_sample_rate,
                event_hash,
                is_archived: event.is_archived.unwrap_or_default().into(),
                ip_address_v4,
                ip_address_v6,
                offset,
                os_name: event.contexts.os.name.unwrap_or_default(),
                os_version: event.contexts.os.version.unwrap_or_default(),
                partition,
                platform: event.platform.unwrap_or("javascript".to_string()),
                project_id: replay_message.project_id,
                release: event.release.unwrap_or_default(),
                replay_id: event.replay_id,
                replay_start_timestamp: event.replay_start_timestamp.map(|v| v as u32),
                replay_type: event.replay_type.unwrap_or_default(),
                retention_days: replay_message.retention_days,
                sdk_name: event.sdk.name.unwrap_or_default(),
                sdk_version: event.sdk.version.unwrap_or_default(),
                segment_id: event.segment_id,
                timestamp: event.timestamp as u32,
                trace_ids: event.trace_ids.unwrap_or_default(),
                urls: event.urls.unwrap_or_default(),
                user,
                user_email: event.user.email.unwrap_or_default(),
                user_id: user_id.unwrap_or_default(),
                user_name: event.user.username.unwrap_or_default(),
                title,
                tags_key,
                tags_value: tags_value
                    .into_iter()
                    .map(|s| s.unwrap_or_default())
                    .collect(),
                ..Default::default()
            }])
        }
        ReplayPayload::EventLinkEvent(event) => Ok(vec![ReplayRow {
            debug_id: event.debug_id,
            error_id: event.error_id,
            error_sample_rate: -1.0,
            event_hash: event.event_hash,
            fatal_id: event.fatal_id,
            info_id: event.info_id,
            offset,
            partition,
            platform: "javascript".to_string(),
            project_id: replay_message.project_id,
            replay_id: replay_message.replay_id,
            retention_days: replay_message.retention_days,
            session_sample_rate: -1.0,
            timestamp: event.timestamp as u32,
            warning_id: event.warning_id,
            ..Default::default()
        }]),
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

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum UserId {
    String(String),
    Number(u64),
}

#[derive(Debug, Deserialize)]
struct ReplayEvent {
    replay_id: Uuid,
    #[serde(default)]
    contexts: Contexts,
    #[serde(default)]
    dist: Option<String>,
    #[serde(default)]
    environment: Option<String>,
    #[serde(default)]
    event_hash: Option<Uuid>,
    #[serde(default)]
    is_archived: Option<bool>,
    #[serde(default)]
    platform: Option<String>,
    #[serde(default)]
    release: Option<String>,
    #[serde(default)]
    replay_start_timestamp: Option<f64>,
    #[serde(default)]
    replay_type: Option<String>,
    #[serde(default)]
    sdk: Version,
    #[serde(default)]
    segment_id: Option<u16>,
    timestamp: f64,
    #[serde(default)]
    urls: Option<Vec<String>>,
    #[serde(default)]
    user: User,
    #[serde(default)]
    trace_ids: Option<Vec<Uuid>>,
    #[serde(default)]
    error_ids: Option<Vec<Uuid>>,
    #[serde(default)]
    tags: Vec<(String, Option<String>)>,
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
    brand: Option<String>,
    #[serde(default)]
    family: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    name: Option<String>,
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
    username: Option<String>,
    #[serde(default)]
    user_id: Option<UserId>,
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    ip_address: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct Version {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    version: Option<String>,
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
pub struct ReplayRow {
    browser_name: String,
    browser_version: String,
    click_alt: String,
    click_aria_label: String,
    click_class: Vec<String>,
    click_component_name: String,
    click_id: String,
    click_is_dead: u8,
    click_is_rage: u8,
    click_node_id: u32,
    click_role: String,
    click_tag: String,
    click_testid: String,
    click_text: String,
    click_title: String,
    debug_id: Uuid,
    device_brand: String,
    device_family: String,
    device_model: String,
    device_name: String,
    dist: String,
    environment: String,
    error_id: Uuid,
    error_ids: Vec<Uuid>,
    error_sample_rate: f64,
    event_hash: Uuid,
    fatal_id: Uuid,
    info_id: Uuid,
    ip_address_v4: Option<Ipv4Addr>,
    ip_address_v6: Option<Ipv6Addr>,
    is_archived: u8,
    offset: u64,
    os_name: String,
    os_version: String,
    partition: u16,
    platform: String,
    project_id: u64,
    release: String,
    replay_id: Uuid,
    replay_start_timestamp: Option<u32>,
    replay_type: String,
    retention_days: u16,
    sdk_name: String,
    sdk_version: String,
    segment_id: Option<u16>,
    session_sample_rate: f64,
    #[serde(rename = "tags.key")]
    tags_key: Vec<String>,
    #[serde(rename = "tags.value")]
    tags_value: Vec<String>,
    timestamp: u32,
    title: Option<String>,
    trace_ids: Vec<Uuid>,
    urls: Vec<String>,
    user_email: String,
    user_id: String,
    user_name: String,
    user: String,
    warning_id: Uuid,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;
    use rust_arroyo::backends::kafka::types::KafkaPayload;
    use std::{str::FromStr, time::SystemTime};

    #[test]
    fn test_parse_replay_event() {
        let payload = r#"{
            "contexts": {
                "browser": {
                    "name": "browser",
                    "version": "v1"
                },
                "device": {
                    "brand": "brand",
                    "family": "family",
                    "model": "model",
                    "name": "name"
                },
                "os": {
                    "name": "os",
                    "version": "v1"
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
                "version": "v1"
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
                ["transaction.name", null]
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

        let rows = deserialize_message(data.as_bytes(), 0, 0).unwrap();
        let replay_row = rows.first().unwrap();

        // Columns in the critical path.
        assert_eq!(&replay_row.browser_name, "browser");
        assert_eq!(&replay_row.browser_version, "v1");
        assert_eq!(&replay_row.device_brand, "brand");
        assert_eq!(&replay_row.device_family, "family");
        assert_eq!(&replay_row.device_model, "model");
        assert_eq!(&replay_row.device_name, "name");
        assert_eq!(&replay_row.dist, "dist");
        assert_eq!(&replay_row.environment, "environment");
        assert_eq!(&replay_row.os_name, "os");
        assert_eq!(&replay_row.os_version, "v1");
        assert_eq!(&replay_row.platform, "platform");
        assert_eq!(&replay_row.release, "release");
        assert_eq!(&replay_row.replay_type, "buffer");
        assert_eq!(&replay_row.sdk_name, "sdk");
        assert_eq!(&replay_row.sdk_version, "v1");
        assert_eq!(&replay_row.user_email, "email");
        assert_eq!(&replay_row.user_id, "user_id");
        assert_eq!(&replay_row.user_name, "username");
        assert_eq!(&replay_row.user, "user_id");
        assert_eq!(
            replay_row.error_ids,
            vec![Uuid::parse_str("df11e6d952da470386a64340f13151c4").unwrap()]
        );
        assert_eq!(replay_row.error_sample_rate, 1.0);
        assert_eq!(
            replay_row.ip_address_v4,
            Some(Ipv4Addr::from_str("127.0.0.1").unwrap())
        );
        assert_eq!(replay_row.ip_address_v6, None);
        assert_eq!(replay_row.is_archived, 0);
        assert_eq!(replay_row.project_id, 1);
        assert_eq!(replay_row.replay_start_timestamp, Some(1702659277));
        assert_eq!(replay_row.retention_days, 30);
        assert_eq!(
            &replay_row.replay_id,
            &Uuid::parse_str("048aa04be40243948eb3b57089c519ee").unwrap()
        );
        assert_eq!(replay_row.segment_id, Some(0));
        assert_eq!(replay_row.session_sample_rate, 0.5);
        assert_eq!(replay_row.title, None);
        assert_eq!(
            replay_row.trace_ids,
            vec![Uuid::parse_str("2cd798d70f9346089026d2014a826629").unwrap()]
        );
        assert_eq!(replay_row.urls, vec!["urls"]);

        // Default columns - not providable on this event.
        assert_eq!(&replay_row.click_alt, "");
        assert_eq!(&replay_row.click_aria_label, "");
        assert_eq!(&replay_row.click_component_name, "");
        assert_eq!(&replay_row.click_id, "");
        assert_eq!(&replay_row.click_role, "");
        assert_eq!(&replay_row.click_tag, "");
        assert_eq!(&replay_row.click_testid, "");
        assert_eq!(&replay_row.click_text, "");
        assert_eq!(&replay_row.click_title, "");
        assert_eq!(replay_row.click_class, Vec::<String>::new());
        assert_eq!(replay_row.click_is_dead, 0);
        assert_eq!(replay_row.click_is_rage, 0);
        assert_eq!(replay_row.click_node_id, 0);
        assert_eq!(replay_row.debug_id, Uuid::nil());
        assert_eq!(replay_row.error_id, Uuid::nil());
        assert_eq!(replay_row.fatal_id, Uuid::nil());
        assert_eq!(replay_row.info_id, Uuid::nil());
        assert_eq!(replay_row.warning_id, Uuid::nil());
    }

    #[test]
    fn test_parse_replay_event_empty_set() {
        // Test every field defaults to
        let payload = r#"{
            "type": "replay_event",
            "replay_id": "048aa04be40243948eb3b57089c519ee",
            "replay_type": null,
            "segment_id": null,
            "event_hash": null,
            "tags": [
                ["a", "b"],
                ["transaction.name", null]
            ],
            "urls": null,
            "is_archived": null,
            "trace_ids": null,
            "error_ids": null,
            "dist": null,
            "platform": null,
            "timestamp": 1704763370,
            "replay_start_timestamp": null,
            "environment": null,
            "release": null,
            "user": {
                "id": null,
                "username": null,
                "email": null,
                "ip_address": null
            },
            "sdk": {
                "name": null,
                "version": null
            },
            "contexts": {
                "replay": {
                    "error_sample_rate": null,
                    "session_sample_rate": null
                },
                "os": {
                    "name": null,
                    "version": null
                },
                "browser": {
                    "name": null,
                    "version": null
                },
                "device": {
                    "name": null,
                    "brand": null,
                    "family": null,
                    "model": null
                }
            }
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

        let rows = deserialize_message(data.as_bytes(), 0, 0).unwrap();
        let replay_row = rows.first().unwrap();

        // Columns in the critical path.
        assert_eq!(&replay_row.browser_name, "");
        assert_eq!(&replay_row.browser_version, "");
        assert_eq!(&replay_row.device_brand, "");
        assert_eq!(&replay_row.device_family, "");
        assert_eq!(&replay_row.device_model, "");
        assert_eq!(&replay_row.device_name, "");
        assert_eq!(&replay_row.dist, "");
        assert_eq!(&replay_row.environment, "");
        assert_eq!(&replay_row.os_name, "");
        assert_eq!(&replay_row.os_version, "");
        assert_eq!(&replay_row.release, "");
        assert_eq!(&replay_row.replay_type, "");
        assert_eq!(&replay_row.sdk_name, "");
        assert_eq!(&replay_row.sdk_version, "");
        assert_eq!(&replay_row.user_email, "");
        assert_eq!(&replay_row.user_id, "");
        assert_eq!(&replay_row.user_name, "");
        assert_eq!(&replay_row.user, "");
        assert_eq!(replay_row.error_ids, vec![]);
        assert_eq!(replay_row.error_sample_rate, -1.0);
        assert_eq!(replay_row.ip_address_v4, None);
        assert_eq!(replay_row.ip_address_v6, None);
        assert_eq!(replay_row.is_archived, 0);
        assert_eq!(replay_row.platform, "javascript".to_string());
        assert_eq!(replay_row.project_id, 1);
        assert_eq!(replay_row.replay_start_timestamp, None);
        assert_eq!(replay_row.retention_days, 30);
        assert_eq!(
            &replay_row.replay_id,
            &Uuid::parse_str("048aa04be40243948eb3b57089c519ee").unwrap()
        );
        assert_eq!(replay_row.segment_id, None);
        assert_eq!(replay_row.session_sample_rate, -1.0);
        assert_eq!(replay_row.title, None);
        assert_eq!(replay_row.trace_ids, vec![]);
        assert_eq!(replay_row.urls, Vec::<String>::new());

        // Default columns - not providable on this event.
        assert_eq!(&replay_row.click_alt, "");
        assert_eq!(&replay_row.click_aria_label, "");
        assert_eq!(&replay_row.click_component_name, "");
        assert_eq!(&replay_row.click_id, "");
        assert_eq!(&replay_row.click_role, "");
        assert_eq!(&replay_row.click_tag, "");
        assert_eq!(&replay_row.click_testid, "");
        assert_eq!(&replay_row.click_text, "");
        assert_eq!(&replay_row.click_title, "");
        assert_eq!(replay_row.click_class, Vec::<String>::new());
        assert_eq!(replay_row.click_is_dead, 0);
        assert_eq!(replay_row.click_is_rage, 0);
        assert_eq!(replay_row.click_node_id, 0);
        assert_eq!(replay_row.debug_id, Uuid::nil());
        assert_eq!(replay_row.error_id, Uuid::nil());
        assert_eq!(replay_row.fatal_id, Uuid::nil());
        assert_eq!(replay_row.info_id, Uuid::nil());
        assert_eq!(replay_row.warning_id, Uuid::nil());
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
                "component_name": "SignUpButton",
                "event_hash": "b4370ef8d1994e96b5bc719b72afbf49",
                "id": "id",
                "is_dead": 0,
                "is_rage": 1,
                "node_id": 320,
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

        let rows = deserialize_message(data.as_bytes(), 0, 0).unwrap();
        let replay_row = rows.first().unwrap();

        // Columns in the critical path.
        assert_eq!(&replay_row.click_alt, "Alternate");
        assert_eq!(&replay_row.click_aria_label, "Aria-label");
        assert_eq!(&replay_row.click_component_name, "SignUpButton");
        assert_eq!(&replay_row.click_id, "id");
        assert_eq!(&replay_row.click_role, "button");
        assert_eq!(&replay_row.click_tag, "div");
        assert_eq!(&replay_row.click_testid, "");
        assert_eq!(&replay_row.click_text, "Submit");
        assert_eq!(&replay_row.click_title, "title");
        assert_eq!(replay_row.click_class, ["hello", "world"]);
        assert_eq!(replay_row.click_is_dead, 0);
        assert_eq!(replay_row.click_is_rage, 1);
        assert_eq!(replay_row.click_node_id, 320);
        assert_eq!(replay_row.project_id, 1);
        assert_eq!(
            &replay_row.replay_id,
            &Uuid::parse_str("048aa04be40243948eb3b57089c519ee").unwrap()
        );
        assert_eq!(replay_row.retention_days, 30);
        assert_eq!(replay_row.segment_id, None);

        // Default columns - not providable on this event.
        assert_eq!(&replay_row.browser_name, "");
        assert_eq!(&replay_row.browser_version, "");
        assert_eq!(&replay_row.device_brand, "");
        assert_eq!(&replay_row.device_family, "");
        assert_eq!(&replay_row.device_model, "");
        assert_eq!(&replay_row.device_name, "");
        assert_eq!(&replay_row.dist, "");
        assert_eq!(&replay_row.environment, "");
        assert_eq!(&replay_row.os_name, "");
        assert_eq!(&replay_row.os_version, "");
        assert_eq!(&replay_row.release, "");
        assert_eq!(&replay_row.replay_type, "");
        assert_eq!(&replay_row.sdk_name, "");
        assert_eq!(&replay_row.sdk_version, "");
        assert_eq!(&replay_row.user_email, "");
        assert_eq!(&replay_row.user_id, "");
        assert_eq!(&replay_row.user_name, "");
        assert_eq!(&replay_row.user, "");
        assert_eq!(replay_row.debug_id, Uuid::nil());
        assert_eq!(replay_row.error_id, Uuid::nil());
        assert_eq!(replay_row.error_ids, vec![]);
        assert_eq!(replay_row.error_sample_rate, -1.0);
        assert_eq!(replay_row.fatal_id, Uuid::nil());
        assert_eq!(replay_row.info_id, Uuid::nil());
        assert_eq!(replay_row.ip_address_v4, None);
        assert_eq!(replay_row.ip_address_v6, None);
        assert_eq!(replay_row.is_archived, 0);
        assert_eq!(replay_row.platform, "javascript".to_string());
        assert_eq!(replay_row.replay_start_timestamp, None);
        assert_eq!(replay_row.session_sample_rate, -1.0);
        assert_eq!(replay_row.title, None);
        assert_eq!(replay_row.trace_ids, vec![]);
        assert_eq!(replay_row.urls, Vec::<String>::new());
        assert_eq!(replay_row.warning_id, Uuid::nil());
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

        let rows = deserialize_message(data.as_bytes(), 0, 0).unwrap();
        let replay_row = rows.first().unwrap();

        // Columns in the critical path.
        assert_eq!(
            replay_row.debug_id,
            Uuid::parse_str("5d51f0eb1d1244e7a8312d8a248cd987").unwrap()
        );
        assert_eq!(replay_row.info_id, Uuid::nil());
        assert_eq!(replay_row.error_id, Uuid::nil());
        assert_eq!(replay_row.fatal_id, Uuid::nil());
        assert_eq!(replay_row.warning_id, Uuid::nil());
        assert_eq!(
            replay_row.event_hash,
            Uuid::parse_str("b4370ef8d1994e96b5bc719b72afbf49").unwrap()
        );
        assert_eq!(
            replay_row.replay_id,
            Uuid::parse_str("048aa04be40243948eb3b57089c519ee").unwrap()
        );
        assert_eq!(replay_row.project_id, 1);
        assert_eq!(replay_row.retention_days, 30);
        assert_eq!(replay_row.segment_id, None);
        assert_eq!(replay_row.timestamp, 1702659277);

        // Default columns - not providable on this event.
        assert_eq!(&replay_row.browser_name, "");
        assert_eq!(&replay_row.browser_version, "");
        assert_eq!(&replay_row.click_alt, "");
        assert_eq!(&replay_row.click_aria_label, "");
        assert_eq!(&replay_row.click_component_name, "");
        assert_eq!(&replay_row.click_id, "");
        assert_eq!(&replay_row.click_role, "");
        assert_eq!(&replay_row.click_tag, "");
        assert_eq!(&replay_row.click_testid, "");
        assert_eq!(&replay_row.click_text, "");
        assert_eq!(&replay_row.click_title, "");
        assert_eq!(&replay_row.device_brand, "");
        assert_eq!(&replay_row.device_family, "");
        assert_eq!(&replay_row.device_model, "");
        assert_eq!(&replay_row.device_name, "");
        assert_eq!(&replay_row.dist, "");
        assert_eq!(&replay_row.environment, "");
        assert_eq!(&replay_row.os_name, "");
        assert_eq!(&replay_row.os_version, "");
        assert_eq!(&replay_row.release, "");
        assert_eq!(&replay_row.replay_type, "");
        assert_eq!(&replay_row.sdk_name, "");
        assert_eq!(&replay_row.sdk_version, "");
        assert_eq!(&replay_row.user_email, "");
        assert_eq!(&replay_row.user_id, "");
        assert_eq!(&replay_row.user_name, "");
        assert_eq!(&replay_row.user, "");
        assert_eq!(replay_row.click_class, Vec::<String>::new());
        assert_eq!(replay_row.click_is_dead, 0);
        assert_eq!(replay_row.click_is_rage, 0);
        assert_eq!(replay_row.click_node_id, 0);
        assert_eq!(replay_row.error_ids, vec![]);
        assert_eq!(replay_row.error_sample_rate, -1.0);
        assert_eq!(replay_row.ip_address_v4, None);
        assert_eq!(replay_row.ip_address_v6, None);
        assert_eq!(replay_row.is_archived, 0);
        assert_eq!(replay_row.platform, "javascript".to_string());
        assert_eq!(replay_row.replay_start_timestamp, None);
        assert_eq!(replay_row.session_sample_rate, -1.0);
        assert_eq!(replay_row.title, None);
        assert_eq!(replay_row.trace_ids, vec![]);
        assert_eq!(replay_row.urls, Vec::<String>::new());
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

        let rows = deserialize_message(data.as_bytes(), 0, 0).unwrap();
        let replay_row = rows.first().unwrap();

        assert_eq!(replay_row.is_archived, 1);
        assert_eq!(replay_row.project_id, 1);
        assert_eq!(replay_row.retention_days, 30);
        assert_eq!(replay_row.segment_id, None);
        assert_eq!(replay_row.timestamp, 1702659277);
        assert_eq!(
            replay_row.replay_id,
            Uuid::parse_str("048aa04be40243948eb3b57089c519ee").unwrap()
        );

        assert_eq!(&replay_row.browser_name, "");
        assert_eq!(&replay_row.browser_version, "");
        assert_eq!(&replay_row.click_alt, "");
        assert_eq!(&replay_row.click_aria_label, "");
        assert_eq!(&replay_row.click_component_name, "");
        assert_eq!(&replay_row.click_id, "");
        assert_eq!(&replay_row.click_role, "");
        assert_eq!(&replay_row.click_tag, "");
        assert_eq!(&replay_row.click_testid, "");
        assert_eq!(&replay_row.click_text, "");
        assert_eq!(&replay_row.click_title, "");
        assert_eq!(&replay_row.device_brand, "");
        assert_eq!(&replay_row.device_family, "");
        assert_eq!(&replay_row.device_model, "");
        assert_eq!(&replay_row.device_name, "");
        assert_eq!(&replay_row.dist, "");
        assert_eq!(&replay_row.environment, "");
        assert_eq!(&replay_row.os_name, "");
        assert_eq!(&replay_row.os_version, "");
        assert_eq!(&replay_row.release, "");
        assert_eq!(&replay_row.replay_type, "");
        assert_eq!(&replay_row.sdk_name, "");
        assert_eq!(&replay_row.sdk_version, "");
        assert_eq!(&replay_row.user_email, "");
        assert_eq!(&replay_row.user_id, "");
        assert_eq!(&replay_row.user_name, "");
        assert_eq!(&replay_row.user, "");
        assert_eq!(replay_row.click_class, Vec::<String>::new());
        assert_eq!(replay_row.click_is_dead, 0);
        assert_eq!(replay_row.click_is_rage, 0);
        assert_eq!(replay_row.click_node_id, 0);
        assert_eq!(replay_row.debug_id, Uuid::nil());
        assert_eq!(replay_row.error_id, Uuid::nil());
        assert_eq!(replay_row.error_ids, vec![]);
        assert_eq!(replay_row.error_sample_rate, -1.0);
        assert_eq!(replay_row.fatal_id, Uuid::nil());
        assert_eq!(replay_row.info_id, Uuid::nil());
        assert_eq!(replay_row.ip_address_v4, None);
        assert_eq!(replay_row.ip_address_v6, None);
        assert_eq!(replay_row.platform, "javascript".to_string());
        assert_eq!(replay_row.replay_start_timestamp, None);
        assert_eq!(replay_row.session_sample_rate, -1.0);
        assert_eq!(replay_row.title, None);
        assert_eq!(replay_row.trace_ids, vec![]);
        assert_eq!(replay_row.urls, Vec::<String>::new());
        assert_eq!(replay_row.warning_id, Uuid::nil());
    }

    #[test]
    fn test_e2e() {
        // We're just testing the consumer interface works. The payload is irrelevant so
        // long as it doesn't fail.
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
        process_message(payload, meta, &ProcessorConfig::default())
            .expect("The message should be processed");
    }
}
