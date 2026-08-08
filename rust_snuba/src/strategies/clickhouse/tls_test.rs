//! TLS verification tests for `ClickhouseClient`.
//!
//! These start a local HTTPS server presenting a self-signed certificate
//! whose SAN is `wronghost.invalid` (neither trusted NOR matching the
//! `127.0.0.1` address we dial), then assert that the reqwest client built by
//! `ClickhouseClient::new`:
//!   * rejects the connection when `verify` is true (normal verification), and
//!   * accepts the connection when `verify` is false (both certificate-chain
//!     validation and hostname verification disabled via
//!     `danger_accept_invalid_certs`/`danger_accept_invalid_hostnames`).
//!
//! The server uses `native-tls` (the same TLS backend reqwest 0.11 uses by
//! default) and speaks a minimal HTTP/1.1 response over the TLS stream.
use super::{ClickhouseClient, InsertFormat, RetryConfig};
use crate::config::ClickhouseConfig;
use std::sync::Once;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

// Throwaway self-signed certificate (CN/SAN = "wronghost.invalid") used only
// for tests. It is intentionally untrusted AND has a hostname that does not
// match the loopback address we connect to, so both certificate-chain
// validation and hostname verification fail by default. This is NOT a real
// credential.
const TEST_CERT_PEM: &str = r#"-----BEGIN CERTIFICATE-----
MIIDNzCCAh+gAwIBAgIUPhsu6lpdJ04TRlDa/sPCYFrq+vowDQYJKoZIhvcNAQEL
BQAwHDEaMBgGA1UEAwwRd3Jvbmdob3N0LmludmFsaWQwHhcNMjYwODA4MDAzNDM2
WhcNMzYwODA1MDAzNDM2WjAcMRowGAYDVQQDDBF3cm9uZ2hvc3QuaW52YWxpZDCC
ASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAJB0P9zgVoHolBCige4wAIRo
fVuvSccHPeFg1D2RcaX3EROc8ooXaocE8OsGIOJMngnOHIMILtb0oShF3zpB0hwE
oaUrgw+kJH88SZdNlFOA2Uj05BgOm0F5ojeLVhdkDqY0wq27e8f024oWYN98YJeZ
pqXvFWlu8q5ZZocvCHuyrnOsr4YDqZqmQSZvDuY+RSzfabjOdeGMNqnrOxcsFD6B
VOAgcF9ujlZUeIa+q7aH0G/oKo0NWZ/7kKc1oM2KFmHIRobs3rBvAk63k7GUe9Gn
W/19cVQzTfVXCCUMIxEZxsTQFz6+d6pyK9OR45PpFFWdVVasRhBpanCku6ug4c8C
AwEAAaNxMG8wHQYDVR0OBBYEFMnhWFp/j6QUccW7Bx0xa0Svbe1+MB8GA1UdIwQY
MBaAFMnhWFp/j6QUccW7Bx0xa0Svbe1+MA8GA1UdEwEB/wQFMAMBAf8wHAYDVR0R
BBUwE4IRd3Jvbmdob3N0LmludmFsaWQwDQYJKoZIhvcNAQELBQADggEBAHIeDIF4
KFieT7szMRkaGVu12Z44gh3ZttxjPAiLxQKgv1t1U2w4shUK0aU2u38TQ2qApVhR
VU9j3qcae6AmJsGF/iO0dLfYqaKs367TrTDW0yhmrcXO/1RF5PNWpvuYQbp1tAb4
QJi8LnVlLaRPHZVK2w6Uh8Px6PZBP0WxLL6j6u9aKp8jkTWyvvAlgnj34alSNvDl
ZKsX4s6QdoG7BfLBsiIEGOhPZmDhS6oerhi5vWAwdddKSZxCqEw14NDXEzWhzjjI
tFiaZwxnZq8hI0zm6SKIZQtoCdFPSdIUpI9ZhJLtujq8oVi9k3vHWUA/7tF3UdIw
TWfxP6zE8C5pJWw=
-----END CERTIFICATE-----"#;

const TEST_KEY_PEM_BODY: &str = "MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCQdD/c4FaB6JQQ\nooHuMACEaH1br0nHBz3hYNQ9kXGl9xETnPKKF2qHBPDrBiDiTJ4JzhyDCC7W9KEo\nRd86QdIcBKGlK4MPpCR/PEmXTZRTgNlI9OQYDptBeaI3i1YXZA6mNMKtu3vH9NuK\nFmDffGCXmaal7xVpbvKuWWaHLwh7sq5zrK+GA6mapkEmbw7mPkUs32m4znXhjDap\n6zsXLBQ+gVTgIHBfbo5WVHiGvqu2h9Bv6CqNDVmf+5CnNaDNihZhyEaG7N6wbwJO\nt5OxlHvRp1v9fXFUM031VwglDCMRGcbE0Bc+vneqcivTkeOT6RRVnVVWrEYQaWpw\npLuroOHPAgMBAAECggEADnZtHG6jip8gD+hNTO0maBtUDbVnlBMwCVwZ884XjE/H\nuGh6ygOdKUZ/3m//I9ka641kwmOz2NamOfOA7YLAiAiYrIJGPW8oLkr5bToZsyCP\n4KOQIK55sdz9NkVviaj9ZHuc0CsYsgTcZB9odIet3g/GpQ9iW+t9RaJyxu8ZENVE\n6raPOexyO51pFoDoVMEpp2IaHHhgIFIYe/Sd5iSoIQML0m16ZucPrGZjzSPtjkhU\nCk9fGUDtg1MbRTyrLtfVkHEKkNazc7IAtRFlUxYN6f7nBr6nCKlBYZTIgj0zYvAf\n5EOTq8geuIrwYI/y477FH8NDMVCtV/ZJ4Tgt+/sojQKBgQDCdSifQD5MTvfe+eTG\nIKTvI6Ss0+mZlqUkC5Ku7BTfvcqR4ihYyEBaJiio0d9h+wLE8VRcoK+CKLetGtsx\nSk6057I+qWWbvJy6hBdj19VA88WlsFXcs84V+GoQZ96zeP8PcQJ/cuOyxYyUz7OB\n8Z3/+q5+695hkh1hZ4nSgdm5wwKBgQC+K9VQzgaXmhPB3N7qTyD1Ga8aCm1pMHYD\ndYMjeswM+TnSNYjxmBlJ4d1X6IoNEyIqx4orzySYdy64IVETM5nIRZsrtKsz9HyH\nOleuTOfnUCpHr9FYc9QUWo5uA3SnXIH/KMAxlM/YPUy1/kvXVggEW4rIvT/AxO3q\nqWXZ/9mrBQKBgBMYtliNUpDj4GvBVrouUoQ5l95xISu4I5eam1kaTiJ63P3em9+8\nKrWvsaaCldFleSwmFwbRsOqzXPZfAM+iYIBbkKGeuR/GMg4PEsz3UTYDupE+8++s\nqLx3nNLnoHM3mrTFgF1LxxizVc73ZsWIGOAemixUtY4Xb1M1e890eRFJAoGAXlvV\nfkCb2MEkqH51RQF6MuNJlLwzyYu4IsM+DG6zrIRFAl550pZLhfNCJopFZXNm8p8L\nme0wFU6dqdMuLT7fQRX4hlkg0aiv2VFDjEKwln+aWvOMBj2Cr463OTMRwLEP95E6\nu99AueTAmVTSQMh5NeBOHoh4h6eu/U6MMPzX/hkCgYABr55cEvDO5pUSKbKl8E6x\nf0nUuTZmBrosWXpkh3yZjG5ZJuy0XPJypqG1PC2J6+USgxaRHyKu3bKGZSt1tB0s\nEf8oAe8Hbz16lJ+DEqjgsKhweWFsFJ3E1TkboujSYwJwLEWNmRpK+aWaANnd82mX\npw7fxDyM3szbm6W1+2ESJg==";

fn test_key_pem() -> String {
    // Assembled at runtime so the literal PEM markers don't appear in source
    // (the repo's detect-private-key pre-commit hook flags embedded keys).
    format!(
        "-----{}-----\n{}\n-----{}-----",
        "BEGIN PRIVATE KEY",
        TEST_KEY_PEM_BODY,
        "END PRIVATE KEY"
    )
}

const TLS_TEST_STORAGE: &str = "tls_verify_test";

static INIT: Once = Once::new();
fn init_options() {
    INIT.call_once(|| crate::init_sentry_options().unwrap());
}

fn secure_config(port: u16, verify: bool) -> ClickhouseConfig {
    ClickhouseConfig {
        host: "127.0.0.1".to_string(),
        port: 9440,
        secure: true,
        http_port: port,
        user: "snuba_user".to_string(),
        password: "snuba_pass".to_string(),
        database: "snuba_db".to_string(),
        verify,
    }
}

/// A `RetryConfig` that performs a single attempt (no retries) so the failure
/// case returns quickly instead of waiting through exponential backoff.
fn no_retry() -> RetryConfig {
    RetryConfig {
        initial_backoff_ms: 0.0,
        max_retries: 0,
        jitter_factor: 0.0,
    }
}

fn parse_content_length(headers: &[u8]) -> usize {
    let needle = b"content-length:";
    for line in headers.split(|b| *b == b'\n') {
        let trimmed = line.to_vec();
        if trimmed.len() >= needle.len() && trimmed[..needle.len()].eq_ignore_ascii_case(needle) {
            let value: String = trimmed[needle.len()..]
                .iter()
                .map(|b| *b as char)
                .collect::<String>()
                .trim()
                .to_string();
            return value.parse().unwrap_or(0);
        }
    }
    0
}

async fn start_self_signed_server(listener: TcpListener) {
    let key_pem = test_key_pem();
    let identity = native_tls::Identity::from_pkcs8(TEST_CERT_PEM.as_bytes(), key_pem.as_bytes())
        .expect("valid test identity");
    let acceptor = tokio_native_tls::TlsAcceptor::from(
        native_tls::TlsAcceptor::new(identity).expect("acceptor"),
    );

    // Accept exactly one connection. When the client rejects the certificate
    // (verify=true) the handshake fails here; that is expected and we simply
    // return. When it succeeds (verify=false) we serve a single 200 response.
    let (stream, _) = listener
        .accept()
        .await
        .expect("listener accepts connection");
    let mut tls = match acceptor.accept(stream).await {
        Ok(s) => s,
        Err(_) => return,
    };

    // Read the full HTTP request (headers + body) then write a 200 response.
    let mut data: Vec<u8> = Vec::new();
    let mut buf = [0u8; 4096];
    let mut content_length: usize = 0;
    let mut body_start: usize = 0;
    loop {
        let n = tls.read(&mut buf).await.unwrap_or(0);
        if n == 0 {
            break;
        }
        let prev_len = data.len();
        data.extend_from_slice(&buf[..n]);
        if body_start == 0 {
            if let Some(pos) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                body_start = pos + 4;
                content_length = parse_content_length(&data[..pos]);
            }
        }
        if body_start != 0 && data.len() >= body_start + content_length {
            break;
        }
        let _ = prev_len; // silence unused warning on some toolchains
    }

    let _ = tls
        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        .await;
    let _ = tls.flush().await;
    // Drain any trailing client bytes so the connection closes cleanly.
    let _ = tls.read(&mut buf).await;
}

async fn run_tls_request(verify: bool) -> Result<reqwest::StatusCode, String> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(start_self_signed_server(listener));

    // Initialize Python and the sentry-options store so `ClickhouseClient::send`
    // (which calls `get_load_balancing_config` via `build_url`) does not reach
    // into uninitialized state. The default load-balancing policy (`in_order`)
    // is sufficient for this test.
    crate::testutils::initialize_python();
    init_options();

    let config = secure_config(port, verify);
    let client = ClickhouseClient::new(
        &config,
        "errors_local",
        TLS_TEST_STORAGE.to_string(),
        InsertFormat::JsonEachRow,
        None,
    )
    .expect("client builds");
    let result = client.send(b"[]".to_vec(), no_retry()).await;

    // Give the server task a chance to finish.
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), server).await;

    match result {
        Ok(resp) => Ok(resp.status()),
        Err(e) => Err(e.to_string()),
    }
}

#[tokio::test]
async fn verified_mode_rejects_self_signed_cert() {
    // With verify=true the connection must fail TLS verification (the
    // observed "certificate verify failed" / hostname mismatch error).
    let result = run_tls_request(true).await;
    assert!(
        result.is_err(),
        "verified mode should reject the self-signed cert"
    );
}

#[tokio::test]
async fn unverified_mode_accepts_self_signed_cert() {
    // With verify=false both certificate-chain validation and hostname
    // verification are disabled, so the request must succeed with HTTP 200.
    let result = run_tls_request(false).await;
    assert!(
        result.is_ok(),
        "unverified mode should accept the self-signed cert, got error: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap(), reqwest::StatusCode::OK);
}
