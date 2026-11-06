pub mod chunker;
pub mod llm;
pub mod stt;
pub mod tts;

/// Percent-encodes a query value (RFC 3986 unreserved characters pass through).
pub(crate) fn urlenc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Opens a Sarvam-style WebSocket with one auth header.
pub(crate) async fn ws_connect(
    url: &str,
    header: &'static str,
    key: &str,
    timeout: std::time::Duration,
) -> anyhow::Result<(WsStream, Option<String>)> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut req = url.into_client_request()?;
    req.headers_mut().insert(header, key.parse()?);
    let (ws, resp) = tokio::time::timeout(timeout, tokio_tungstenite::connect_async(req))
        .await
        .map_err(|_| anyhow::anyhow!("handshake timed out"))??;
    let rid = ["x-request-id", "request-id", "x-sarvam-request-id"]
        .iter()
        .find_map(|h| resp.headers().get(*h)?.to_str().ok().map(String::from));
    Ok((ws, rid))
}

pub(crate) type WsStream = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
