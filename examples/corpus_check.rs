//! H1 corpus regression: chay stage2 analysis tren moi corpus/<dir> va bao pass/fail.
//! Usage: corpus_check <corpus-root>
//! Moi dir can: iframe.deobfuscated.js (source) + response.decoded.txt (program base64).
//! Ket qua: <dir> PASS/FAIL <loi> — build moi fail la bisect tu ngay cho nao.

use anyhow::{Context, Result, ensure};
use cfsolve::script::Source;
use cfsolve::stage2::analysis;

fn main() -> Result<()> {
    let root = std::env::args()
        .nth(1)
        .context("usage: corpus_check <corpus-root>")?;
    let mut dirs: Vec<_> = std::fs::read_dir(&root)
        .with_context(|| format!("reading {root}"))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect();
    dirs.sort();
    ensure!(!dirs.is_empty(), "khong co dir nao trong {root}");
    let mut pass = 0;
    for dir in &dirs {
        let name = dir.file_name().unwrap().to_string_lossy().to_string();
        let script = dir.join("iframe.deobfuscated.js");
        let program_txt = dir.join("response.decoded.txt");
        if !script.exists() || !program_txt.exists() {
            println!("{name}\tSKIP (thieu file)");
            continue;
        }
        let text = std::fs::read_to_string(&script)?;
        let source = Source::try_new(&text).context("parse source")?;
        let encoded = std::fs::read_to_string(&program_txt)?;
        use base64::Engine;
        let program = match base64::engine::general_purpose::STANDARD.decode(encoded.trim()) {
            Ok(p) => p,
            Err(e) => {
                println!("{name}\tFAIL base64-decode {e}");
                continue;
            }
        };
        let started = std::time::Instant::now();
        match analysis::analyze(&source, &program) {
            Ok(a) => {
                let probes = a.plan().probes().len();
                println!("{name}\tPASS {probes} probes ({}ms)", started.elapsed().as_millis());
                pass += 1;
            }
            Err(e) => {
                let mut chain = String::new();
                for cause in e.chain() {
                    chain.push_str(&format!(" | {cause}"));
                }
                println!("{name}\tFAIL{chain}");
            }
        }
    }
    println!("-- {pass}/{} pass --", dirs.len());
    Ok(())
}
