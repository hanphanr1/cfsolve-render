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

struct Prefetch {
    sitekey: String,
    url_host: String,
    want: usize,
    ttl: Duration,
    max_minters: usize,
    queue: Mutex<VecDeque<QueuedToken>>,
}

fn prefetch_cfg() -> Option<&'static Prefetch> {
    static P: LazyLock<Option<Prefetch>> = LazyLock::new(|| {
        let sitekey = std::env::var("PREFETCH_SITEKEY").ok()?;
        let url = std::env::var("PREFETCH_URL").ok()?;
        let host = Url::parse(&url).ok()?.host_str()?.to_string();
        let want: usize = std::env::var("PREFETCH_N").ok().and_then(|v| v.parse().ok()).unwrap_or(3);
        let ttl_s: u64 = std::env::var("PREFETCH_TTL_S").ok().and_then(|v| v.parse().ok()).unwrap_or(220);
        let max_minters: usize =
            std::env::var("PREFETCH_MINTERS").ok().and_then(|v| v.parse().ok()).unwrap_or(3);
        if want == 0 {
            return None;
        }
        Some(Prefetch {
            sitekey,
            url_host: host,
            want,
            ttl: Duration::from_secs(ttl_s),
            max_minters,
            queue: Mutex::new(VecDeque::new()),
        })
    });
    P.as_ref()
}

fn queue_len(p: &Prefetch) -> usize {
    p.queue.lock().map(|q| q.len()).unwrap_or(0)
}

fn queue_ages(p: &Prefetch) -> Vec<u64> {
    p.queue
        .lock()
        .map(|q| q.iter().map(|t| t.minted.elapsed().as_secs()).collect())
        .unwrap_or_default()
}

fn pop_fresh(p: &Prefetch) -> Option<String> {
    let mut q = p.queue.lock().ok()?;
    while let Some(front) = q.front() {
        if front.minted.elapsed() > p.ttl {
            q.pop_front();
            continue;
        }
        return q.pop_front().map(|t| t.token);
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
    let Some(p) = prefetch_cfg() else { return };
    eprintln!(
        "[websolve] prefetch ON: sitekey={} host={} n={} ttl={:?} minters={}",
        p.sitekey,
        p.url_host,
        p.want,
        p.ttl,
        p.max_minters
    );
    for worker in 0..p.max_minters {
        let sitekey = p.sitekey.clone();
        let url = std::env::var("PREFETCH_URL").unwrap_or_default();
        let url = Arc::new(url);
        let p_ref: &'static Prefetch = p;
        tokio::spawn(async move {
            loop {
                let need = {
                    let mut q = match p_ref.queue.lock() {
                        Ok(q) => q,
                        Err(_) => {
                            tokio::time::sleep(Duration::from_millis(500)).await;
                            continue;
                        }
                    };
                    // bot token qua TTL
                    while let Some(front) = q.front() {
                        if front.minted.elapsed() > p_ref.ttl {
                            q.pop_front();
                        } else {
                            break;
                        }
                    }
                    p_ref.want.saturating_sub(q.len())
                };
                if need == 0 {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    continue;
                }
                if MINTING.load(Ordering::Relaxed) >= p_ref.max_minters {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
                MINTING.fetch_add(1, Ordering::Relaxed);
                let started = Instant::now();
                match mint(&sitekey, &url, None).await {
                    Ok(token) => {
                        SOLVED.fetch_add(1, Ordering::Relaxed);
                        if let Ok(mut q) = p_ref.queue.lock() {
                            if q.len() < p_ref.want {
                                q.push_back(QueuedToken { token, minted: Instant::now() });
                            }
                        }
                        eprintln!("[minter{worker}] OK in {:?} (queue={})", started.elapsed(), queue_len(p_ref));
                    }
                    Err(e) => {
                        FAILED.fetch_add(1, Ordering::Relaxed);
                        eprintln!("[minter{worker}] FAIL: {e}");
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
    if let Some(p) = prefetch_cfg() {
        let host_ok = Url::parse(&req.url)
            .ok()
            .and_then(|u| u.host_str().map(|h| h.eq_ignore_ascii_case(&p.url_host)))
            .unwrap_or(false);
        if host_ok && req.sitekey == p.sitekey && req.action.is_none() && req.cdata.is_none() && req.proxy.is_none()
        {
            if let Some(token) = pop_fresh(p) {
                return (200, format!(
                    r#"{{"success":true,"elapsed_ms":{},"from_queue":true,"token":{}}}"#,
                    started.elapsed().as_millis(),
                    serde_json::to_string(&token).unwrap_or_default()
                ));
            }
        }
    }
    // duong thong thuong: mint truc tiep
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
    let (depth, ages) = match prefetch_cfg() {
        Some(p) => (queue_len(p), queue_ages(p)),
        None => (0, Vec::new()),
    };
    format!(
        r#"{{"ok":true,"queue_depth":{},"queue_ages_s":{:?},"minting":{},"solved":{},"failed":{},"profile":"{}"}}"#,
        depth,
        ages,
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
