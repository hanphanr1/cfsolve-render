// websolve v2: HTTP wrapper cho cfsolve + PREFETCH QUEUE.
// - POST /solve {sitekey,url,action?,cdata?,proxy?} -> {success,elapsed_ms,token|message,from_queue?}
// - Neu request khop PREFETCH_SITEKEY/PREFETCH_URL (va khong action/cdata) -> pop token tuoi tu queue (~0s),
//   nguoc lai mint truc tiep. Queue do background minter giu moi (PREFETCH_N token, TTL PREFETCH_TTL_S).
// - GET /health -> {ok, queue_depth, queue_ages_s, solved, failed, minting}
// Khong dung them dependency (tu viet HTTP/1.1 tren tokio). PORT default 8080 (Render dat 10000).

use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use cfsolve::{Browser, Options, PageSource, Turnstile, Url, Widget};

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

static SOLVED: AtomicU64 = AtomicU64::new(0);
static FAILED: AtomicU64 = AtomicU64::new(0);
static MINTING: AtomicUsize = AtomicUsize::new(0);

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

// ---------- prefetch queue ----------

struct QueuedToken {
    token: String,
    minted: Instant,
}

// ---------- multi-target prefetch ----------
// PREFETCH_TARGETS = JSON [{"sitekey":"0x..","url":"https://..","n":12}, ...]
// moi muc tieu co queue rieng; /solve pop theo (sitekey, host). Fallback: env don cu
// PREFETCH_SITEKEY/PREFETCH_URL/PREFETCH_N van hoat dong.

struct Target {
    sitekey: String,
    url: String,
    host: String,
    want: usize,
    queue: Mutex<VecDeque<QueuedToken>>,
}

fn targets() -> &'static Vec<Target> {
    static T: LazyLock<Vec<Target>> = LazyLock::new(|| {
        let mut defs: Vec<(String, String, usize)> = Vec::new();
        if let Ok(raw) = std::env::var("PREFETCH_TARGETS") {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&raw) {
                if let Some(arr) = v.as_array() {
                    for item in arr {
                        let sk = item.get("sitekey").and_then(|x| x.as_str()).unwrap_or("").to_string();
                        let u = item.get("url").and_then(|x| x.as_str()).unwrap_or("").to_string();
                        let n = item.get("n").and_then(|x| x.as_u64()).unwrap_or(6) as usize;
                        if !sk.is_empty() && !u.is_empty() {
                            defs.push((sk, u, n));
                        }
                    }
                }
            }
        }
        if defs.is_empty() {
            if let (Ok(sk), Ok(u)) = (std::env::var("PREFETCH_SITEKEY"), std::env::var("PREFETCH_URL")) {
                if !sk.is_empty() && !u.is_empty() {
                    let n: usize = std::env::var("PREFETCH_N").ok().and_then(|v| v.parse().ok()).unwrap_or(3);
                    defs.push((sk, u, n));
                }
            }
        }
        defs.into_iter()
            .filter_map(|(sitekey, url, want)| {
                let host = Url::parse(&url).ok()?.host_str()?.to_string();
                Some(Target { sitekey, url, host, want, queue: Mutex::new(VecDeque::new()) })
            })
            .collect()
    });
    &T
}

fn ttl() -> Duration {
    Duration::from_secs(std::env::var("PREFETCH_TTL_S").ok().and_then(|v| v.parse().ok()).unwrap_or(220))
}

fn max_minters() -> usize {
    std::env::var("PREFETCH_MINTERS").ok().and_then(|v| v.parse().ok()).unwrap_or(3)
}

fn queue_len(t: &Target) -> usize {
    t.queue.lock().map(|q| q.len()).unwrap_or(0)
}

fn queue_ages(t: &Target) -> Vec<u64> {
    t.queue.lock().map(|q| q.iter().map(|x| x.minted.elapsed().as_secs()).collect()).unwrap_or_default()
}

fn pop_fresh(sitekey: &str, host: &str) -> Option<String> {
    let t = targets().iter().find(|t| t.sitekey == sitekey && t.host.eq_ignore_ascii_case(host))?;
    let mut q = t.queue.lock().ok()?;
    while let Some(front) = q.front() {
        if front.minted.elapsed() > ttl() {
            q.pop_front();
            continue;
        }
        return q.pop_front().map(|x| x.token);
    }
    None
}

async fn mint(sitekey: &str, url: &str, proxy: Option<String>) -> Result<String, String> {
    let b = browser().map_err(|e| e)?;
    let url = Url::parse(url).map_err(|e| e.to_string())?;
    let t = Turnstile {
        url,
        page: PageSource::Blank,
        widget: Some(Widget { sitekey: sitekey.to_string(), action: None, cdata: None }),
    };
    let options = Options { proxy, sink: None };
    b.solve_turnstile(t, &options).await.map_err(|e| e.to_string())
}

fn start_minters() {
    let list = targets();
    if list.is_empty() {
        return;
    }
    eprintln!(
        "[websolve] prefetch ON: {} target(s) ttl={:?} minters={} cap={}",
        list.len(),
        ttl(),
        max_minters(),
        list.iter().map(|t| format!("{}@{}x{}", &t.sitekey[..8.min(t.sitekey.len())], t.host, t.want)).collect::<Vec<_>>().join(", ")
    );
    // worker pool chung: moi worker vong qua cac target thieu token ma mint
    for worker in 0..max_minters() {
        let list_ptr: &'static Vec<Target> = list;
        tokio::spawn(async move {
            loop {
                // chon target co ty le thap nhat (queue/want)
                let pick = {
                    let mut best: Option<(usize, f64)> = None;
                    for (i, t) in list_ptr.iter().enumerate() {
                        let need = match t.queue.lock() {
                            Ok(mut q) => {
                                while let Some(f) = q.front() {
                                    if f.minted.elapsed() > ttl() { q.pop_front(); } else { break; }
                                }
                                t.want.saturating_sub(q.len())
                            }
                            Err(_) => 0,
                        };
                        if need == 0 { continue; }
                        let ratio = need as f64 / t.want.max(1) as f64;
                        if best.map(|(_, r)| ratio > r).unwrap_or(true) {
                            best = Some((i, ratio));
                        }
                    }
                    best.map(|(i, _)| i)
                };
                let Some(idx) = pick else {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    continue;
                };
                let t = &list_ptr[idx];
                if MINTING.load(Ordering::Relaxed) >= max_minters() {
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    continue;
                }
                MINTING.fetch_add(1, Ordering::Relaxed);
                let started = Instant::now();
                match mint(&t.sitekey, &t.url, None).await {
                    Ok(token) => {
                        SOLVED.fetch_add(1, Ordering::Relaxed);
                        if let Ok(mut q) = t.queue.lock() {
                            if q.len() < t.want {
                                q.push_back(QueuedToken { token, minted: Instant::now() });
                            }
                        }
                        eprintln!("[minter{worker}] OK {} in {:?} (queue={})", &t.sitekey[..8], started.elapsed(), queue_len(t));
                    }
                    Err(e) => {
                        FAILED.fetch_add(1, Ordering::Relaxed);
                        eprintln!("[minter{worker}] FAIL {}: {e}", &t.sitekey[..8]);
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
                MINTING.fetch_sub(1, Ordering::Relaxed);
            }
        });
    }
}

// ---------- HTTP ----------

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
    let started = Instant::now();
    // duong nhanh: queue cho (sitekey, host) khop va khong action/cdata/proxy
    if req.action.is_none() && req.cdata.is_none() && req.proxy.is_none() {
        if let Some(host) = Url::parse(&req.url).ok().and_then(|u| u.host_str().map(|s| s.to_string())) {
            if let Some(token) = pop_fresh(&req.sitekey, &host) {
                return (200, format!(
                    r#"{{"success":true,"elapsed_ms":{},"from_queue":true,"token":{}}}"#,
                    started.elapsed().as_millis(),
                    serde_json::to_string(&token).unwrap_or_default()
                ));
            }
        }
    }
    // fallback: mint local bang cfsolve
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
    match b.solve_turnstile(t, &options).await {
        Ok(token) => {
            SOLVED.fetch_add(1, Ordering::Relaxed);
            (200, format!(
                r#"{{"success":true,"elapsed_ms":{},"token":{}}}"#,
                started.elapsed().as_millis(),
                serde_json::to_string(&token).unwrap_or_default()
            ))
        }
        Err(e) => {
            FAILED.fetch_add(1, Ordering::Relaxed);
            (200, format!(
                r#"{{"success":false,"elapsed_ms":{},"message":{}}}"#,
                started.elapsed().as_millis(),
                serde_json::to_string(&e.to_string()).unwrap_or_default()
            ))
        }
    }
}

fn err_json(msg: &str) -> String {
    format!(r#"{{"success":false,"message":{}}}"#, serde_json::to_string(msg).unwrap_or_default())
}

fn health_json() -> String {
    let list = targets();
    let qs: Vec<serde_json::Value> = list
        .iter()
        .map(|t| {
            serde_json::json!({
                "sitekey": t.sitekey,
                "host": t.host,
                "want": t.want,
                "depth": queue_len(t),
                "ages_s": queue_ages(t),
            })
        })
        .collect();
    format!(
        r#"{{"ok":true,"mode":"local","targets":{},"minting":{},"solved":{},"failed":{},"profile":"{}"}}"#,
        serde_json::to_string(&qs).unwrap_or_default(),
        MINTING.load(Ordering::Relaxed),
        SOLVED.load(Ordering::Relaxed),
        FAILED.load(Ordering::Relaxed),
        std::env::var("PROFILE").unwrap_or_else(|_| "profiles/safari-26-ios.json".into())
    )
}

async fn serve_conn(stream: tokio::net::TcpStream) {
    let mut reader = BufReader::new(stream);
    let mut head = Vec::with_capacity(1024);
    let mut buf = [0u8; 4096];
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
    match browser() {
        Ok(_) => eprintln!("[websolve] profile ok"),
        Err(e) => eprintln!("[websolve] PROFILE ERROR: {e}"),
    }
    start_minters();
    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(x) => x,
            Err(_) => continue,
        };
        tokio::spawn(serve_conn(stream));
    }
}
