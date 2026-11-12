pub mod chunker;
pub mod llm;
pub mod stt;
pub mod tts;

/// The `User-Agent` of every outbound HTTP client and WebSocket handshake, so gateways (Bifrost) show an app name
/// for us, as they do for the Go engine's `Go-http-client/1.1`. reqwest sends none by default.
pub(crate) const USER_AGENT: &str = "Rust-http-client/1.1";

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
    req.headers_mut().insert("user-agent", USER_AGENT.parse()?);
    // tungstenite's default 128 KiB read + write buffers per socket, times two
    // sockets per call, dominated a call's memory. Audio messages are ~1-2 KB.
    let cfg = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default().read_buffer_size(16 * 1024).write_buffer_size(0);
    let (ws, resp) = tokio::time::timeout(timeout, tokio_tungstenite::connect_async_with_config(req, Some(cfg), false))
        .await
        .map_err(|_| anyhow::anyhow!("handshake timed out"))??;
    let rid =
        ["x-request-id", "request-id", "x-sarvam-request-id"].iter().find_map(|h| resp.headers().get(*h)?.to_str().ok().map(String::from));
    Ok((ws, rid))
}

pub(crate) type WsStream = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
