//! Dograh's MinIO bucket, where it keeps call recordings, transcripts and
//! workflow recordings (api/services/filesystem/minio.py). Only PUT and GET of
//! one object are needed, so requests are signed with AWS Signature V4 by hand
//! (about 40 lines) over the shared HTTP client instead of pulling in an S3 SDK.

use crate::ai::llm;
use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};

/// The region MinIO signs for unless configured otherwise.
const REGION: &str = "us-east-1";

#[derive(Debug, Clone)]
pub struct Storage {
    /// host:port, as Dograh's MINIO_ENDPOINT.
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub bucket: String,
    /// https instead of http.
    pub secure: bool,
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn hmac(key: &[u8], data: &str) -> Vec<u8> {
    let mut m = <Hmac<Sha256> as KeyInit>::new_from_slice(key).expect("hmac takes any key length");
    m.update(data.as_bytes());
    m.finalize().into_bytes().to_vec()
}

/// Percent-encodes an object path the way S3 canonical requests want it:
/// every byte but unreserved characters and '/'.
fn encode_path(p: &str) -> String {
    let mut out = String::with_capacity(p.len());
    for b in p.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

impl Storage {
    fn url(&self, key: &str) -> (String, String) {
        let path = encode_path(&format!("/{}/{key}", self.bucket));
        (format!("{}://{}{path}", if self.secure { "https" } else { "http" }, self.endpoint), path)
    }

    /// The headers AWS Signature V4 adds: `(x-amz-date, x-amz-content-sha256,
    /// authorization)`. Signs content-type, host, x-amz-content-sha256 and
    /// x-amz-date -- what the MinIO Python client (minio/signer.py) signs for the
    /// same request.
    fn sign(&self, method: &str, path: &str, content_type: &str, body: &[u8], now: DateTime<Utc>) -> (String, String, String) {
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let day = now.format("%Y%m%d").to_string();
        let payload_hash = hex(&Sha256::digest(body));

        const SIGNED: &str = "content-type;host;x-amz-content-sha256;x-amz-date";
        let canonical = format!(
            "{method}\n{path}\n\ncontent-type:{content_type}\nhost:{}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n\n{SIGNED}\n{payload_hash}",
            self.endpoint
        );
        let scope = format!("{day}/{REGION}/s3/aws4_request");
        let to_sign = format!("AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}", hex(&Sha256::digest(canonical.as_bytes())));

        let mut key = hmac(format!("AWS4{}", self.secret_key).as_bytes(), &day);
        for part in [REGION, "s3", "aws4_request"] {
            key = hmac(&key, part);
        }
        let auth = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={SIGNED}, Signature={}",
            self.access_key,
            hex(&hmac(&key, &to_sign))
        );
        (amz_date, payload_hash, auth)
    }

    fn request(&self, method: reqwest::Method, key: &str, content_type: &str, body: &[u8]) -> reqwest::RequestBuilder {
        let (url, path) = self.url(key);
        let (date, hash, auth) = self.sign(method.as_str(), &path, content_type, body, Utc::now());
        llm::http()
            .request(method, url)
            .header("Content-Type", content_type)
            .header("X-Amz-Date", date)
            .header("X-Amz-Content-Sha256", hash)
            .header("Authorization", auth)
    }

    /// Uploads `body` as object `key` (path-style URL, as the MinIO client does).
    pub async fn put(&self, key: &str, content_type: &str, body: Vec<u8>) -> Result<()> {
        let resp = self
            .request(reqwest::Method::PUT, key, content_type, &body)
            .body(body)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("minio put {key}: {e}"))?;
        if resp.status() != reqwest::StatusCode::OK {
            let status = resp.status();
            let msg = resp.text().await.unwrap_or_default();
            bail!("minio put {key}: {status}: {}", msg.chars().take(512).collect::<String>());
        }
        Ok(())
    }

    /// Downloads object `key`, at most `max_bytes` of it.
    pub async fn get(&self, key: &str, max_bytes: usize) -> Result<Vec<u8>> {
        // sign() signs content-type, so a GET sends one too.
        let resp = self
            .request(reqwest::Method::GET, key, "application/octet-stream", &[])
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("minio get {key}: {e}"))?;
        if resp.status() != reqwest::StatusCode::OK {
            let status = resp.status();
            let msg = resp.text().await.unwrap_or_default();
            bail!("minio get {key}: {status}: {}", msg.chars().take(512).collect::<String>());
        }
        let body = resp.bytes().await.map_err(|e| anyhow::anyhow!("minio get {key}: {e}"))?;
        if body.len() > max_bytes {
            bail!("minio get {key}: larger than {max_bytes} bytes");
        }
        Ok(body.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    fn storage(endpoint: &str) -> Storage {
        Storage {
            endpoint: endpoint.into(),
            access_key: "minioadmin".into(),
            secret_key: "minioadmin".into(),
            bucket: "voice-audio".into(),
            secure: false,
        }
    }

    /// The expected header was produced by the MinIO Python client Dograh uses
    /// (minio.signer.sign_v4_s3) for the same request: PUT
    /// voice-audio/recordings/42.wav, body "hello", content-type audio/wav,
    /// minioadmin/minioadmin, 2026-10-05 12:00:00 UTC.
    #[test]
    fn signature_matches_the_minio_python_client() {
        let (date, _, auth) = storage("minio:9000").sign(
            "PUT",
            "/voice-audio/recordings/42.wav",
            "audio/wav",
            b"hello",
            Utc.with_ymd_and_hms(2026, 10, 5, 12, 0, 0).unwrap(),
        );
        assert_eq!(date, "20261005T120000Z");
        assert_eq!(
            auth,
            "AWS4-HMAC-SHA256 Credential=minioadmin/20261005/us-east-1/s3/aws4_request, \
             SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, \
             Signature=98334d8941ceb5e9e6ee5186909035642d88ff61052e51f5a178729dcb0fdca0"
        );
    }

    #[test]
    fn paths_are_percent_encoded_but_slashes_stay() {
        assert_eq!(encode_path("/b/recordings/a b+c.wav"), "/b/recordings/a%20b%2Bc.wav");
    }

    /// A one-shot server: records "METHOD path", content-type, auth and body; answers `status`.
    async fn serve(status: u16, reply: &'static str) -> (String, tokio::sync::oneshot::Receiver<String>) {
        let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = lis.local_addr().unwrap().to_string();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut s, _) = lis.accept().await.unwrap();
            let mut buf = vec![0u8; 8192];
            let mut n = 0;
            // Read until the headers (and a small body) arrived.
            loop {
                let r = s.read(&mut buf[n..]).await.unwrap();
                n += r;
                let t = String::from_utf8_lossy(&buf[..n]).to_string();
                if r == 0
                    || t.split_once("\r\n\r\n").is_some_and(|(h, b)| {
                        b.len()
                            >= h.to_lowercase()
                                .split("content-length: ")
                                .nth(1)
                                .and_then(|v| v.split("\r\n").next()?.parse().ok())
                                .unwrap_or(0)
                    })
                {
                    break;
                }
            }
            let _ = tx.send(String::from_utf8_lossy(&buf[..n]).to_string());
            let resp = format!("HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}", reply.len());
            s.write_all(resp.as_bytes()).await.unwrap();
        });
        (addr, rx)
    }

    #[tokio::test]
    async fn put_sends_a_signed_request_and_surfaces_errors() {
        let (addr, req) = serve(200, "").await;
        storage(&addr).put("transcripts/7.txt", "text/plain; charset=utf-8", b"User: hi\n".to_vec()).await.unwrap();
        let r = req.await.unwrap();
        assert!(r.starts_with("PUT /voice-audio/transcripts/7.txt "), "{r}");
        assert!(r.to_lowercase().contains("content-type: text/plain; charset=utf-8"));
        assert!(r.contains("AWS4-HMAC-SHA256 Credential=minioadmin/"));
        assert!(r.ends_with("User: hi\n"));

        let (addr, _) = serve(403, "denied").await;
        let e = storage(&addr).put("x", "audio/wav", vec![]).await.unwrap_err().to_string();
        assert!(e.contains("403") && e.contains("denied"), "{e}");
    }

    #[tokio::test]
    async fn get_returns_the_body_and_enforces_the_size_cap() {
        let (addr, _) = serve(200, "0123456789").await;
        assert_eq!(storage(&addr).get("k", 100).await.unwrap(), b"0123456789");
        let (addr, _) = serve(200, "0123456789").await;
        assert!(storage(&addr).get("k", 5).await.unwrap_err().to_string().contains("larger than 5"));
    }
}
