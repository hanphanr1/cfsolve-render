// websolve: HTTP wrapper cho cfsolve — POST /solve {sitekey,url,action?,cdata?,proxy?} -> token.
// Khong dung them dependency (hyper chi co o internals; o day tu viet HTTP/1.1 nho tren tokio)
// de khong lam thay doi Cargo.lock / mo truong build ban dau.
// Deploy: Railway / Render (xem Dockerfile). PORT default 8080, PROFILE default profiles/safari-26-ios.json.

use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use cfsolve::{Browser, Options, PageSource, Turnstile, Url, Widget};

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

static SOLVED: AtomicU64 = AtomicU64::new(0);
static FAILED: AtomicU64 = AtomicU64::new(0);

fn browser() -> Result<&'static Arc<Browser>, String> {
    static B: LazyLock<Result<Arc<Browser>, String>> = LazyLock::new(|| {
        let path = std::env::var("PROFILE").unwrap_or_else(|_| "profiles/safari-26-ios.json".into());
        match Browser::read(&path) {
            Ok(b) => Ok(Arc::new(b)),
            Err(e) => Err(format!("profile {path}: {e}")),
        }
    });
    B.as_ref().map_err(|e| e.clone())
}

#[derive(serde::Deserialize)]
struct SolveReq {
    sitekey: String,
    url: String,
    action: Option<String>,
    cdata: Option<String>,
    proxy: Option<String>,
}

async fn handle_solve(body: &[u8]) -> (u16, String) {
    let req: SolveReq = match serde_json::from_slice(body) {
        Ok(r) => r,
        Err(e) => return (400, err_json(&format!("bad json: {e}"))),
    };
    let b = match browser() {
        Ok(b) => b,
        Err(e) => return (500, err_json(&e)),
    };
    let url = match Url::parse(&req.url) {
        Ok(u) => u,
        Err(e) => return (400, err_json(&format!("bad url: {e}"))),
    };
    let t = Turnstile {
        url,
        page: PageSource::Blank,
        widget: Some(Widget {
            sitekey: req.sitekey,
            action: req.action,
            cdata: req.cdata,
        }),
    };
    let options = Options { proxy: req.proxy, sink: None };
    let started = std::time::Instant::now();
    match b.solve_turnstile(t, &options).await {
        Ok(token) => {
            SOLVED.fetch_add(1, Ordering::Relaxed);
            let ms = started.elapsed().as_millis();
            (200, format!(
                r#"{{"success":true,"elapsed_ms":{ms},"token":{}}}"#,
                serde_json::to_string(&token).unwrap_or_default()
            ))
        }
        Err(e) => {
            FAILED.fetch_add(1, Ordering::Relaxed);
            let ms = started.elapsed().as_millis();
            (200, format!(
                r#"{{"success":false,"elapsed_ms":{ms},"message":{}}}"#,
                serde_json::to_string(&e.to_string()).unwrap_or_default()
            ))
        }
    }
}

fn err_json(msg: &str) -> String {
    format!(r#"{{"success":false,"message":{}}}"#, serde_json::to_string(msg).unwrap_or_default())
}

fn health_json() -> String {
    format!(
        r#"{{"ok":true,"solved":{},"failed":{},"profile":"{}"}}"#,
        SOLVED.load(Ordering::Relaxed),
        FAILED.load(Ordering::Relaxed),
        std::env::var("PROFILE").unwrap_or_else(|_| "profiles/safari-26-ios.json".into())
    )
}

async fn serve_conn(stream: tokio::net::TcpStream) {
    let mut reader = BufReader::new(stream);
    let mut head = Vec::with_capacity(1024);
    let mut buf = [0u8; 4096];
    // doc headers den \r\n\r\n
    loop {
        match reader.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => {
                head.extend_from_slice(&buf[..n]);
                if head.windows(4).any(|w| w == b"\r\n\r\n") || head.len() > 64 * 1024 {
                    break;
                }
            }
        }
    }
    let head_str = String::from_utf8_lossy(&head);
    let mut lines = head_str.split("\r\n");
    let request_line = lines.next().unwrap_or_default().to_string();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts.next().unwrap_or_default().to_string();
    let mut content_length = 0usize;
    for l in lines {
        if let Some((k, v)) = l.split_once(':') {
            if k.trim().eq_ignore_ascii_case("content-length") {
                content_length = v.trim().parse().unwrap_or(0);
            }
        }
    }
    let header_end = head_str.find("\r\n\r\n").map(|i| i + 4).unwrap_or(head_str.len());
    let mut body: Vec<u8> = head[header_end..].to_vec();
    if body.len() > content_length {
        body.truncate(content_length);
    }
    while body.len() < content_length {
        match reader.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => body.extend_from_slice(&buf[..n]),
        }
    }

    let (status, payload) = if method == "GET" && (path == "/health" || path == "/") {
        (200, health_json())
    } else if method == "POST" && path == "/solve" {
        let (code, p) = handle_solve(&body).await;
        (code, p)
    } else {
        (404, err_json("not found"))
    };
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        _ => "Not Found",
    };
    let resp = format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{payload}",
        payload.len()
    );
    let _ = reader.get_mut().write_all(resp.as_bytes()).await;
    let _ = reader.get_mut().shutdown().await;
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let port: u16 = std::env::var("PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(8080);
    let addr: SocketAddr = std::env::var("BIND_LOCAL")
        .map(|_| ([127, 0, 0, 1], port).into())
        .unwrap_or(([0, 0, 0, 0], port).into());
    let listener = TcpListener::bind(addr).await?;
    eprintln!(
        "[websolve] listening on {addr}, profile={}",
        std::env::var("PROFILE").unwrap_or_else(|_| "profiles/safari-26-ios.json".into())
    );
    // preload profile ngay luc khoi dong de fail-som neu thieu file
    match browser() {
        Ok(_) => eprintln!("[websolve] profile ok"),
        Err(e) => eprintln!("[websolve] PROFILE ERROR: {e}"),
    }
    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(x) => x,
            Err(_) => continue,
        };
        tokio::spawn(serve_conn(stream));
    }
}
