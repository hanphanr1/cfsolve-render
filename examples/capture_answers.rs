//! Builds stored probe answers from a captured session and its plaintext payload.
//!
//! The capture service records a real Chrome solve: `rch-1-*.html` (the iframe document, with
//! the plaintext hook), the response program, and `plain-ok-rN.json` (a payload before the
//! encoder runs). This builds the `answers.json` records those sessions establish, so reference
//! probes the tracked profile does not cover can be replayed.
//!
//! The program is either a decoded `response.decoded.txt`, or the raw POST body in
//! `post-body.bin` with its URL in `post-url.txt`, which this decodes with the session's ray.
//!
//! Usage: capture_answers <session-directory> <plain-payload.json> <output.json>

use std::path::Path;
use std::{env, fs};

use anyhow::{Context, Result, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use cfsolve::challenge::{decode_response, ray};
use cfsolve::js::Payload;
use cfsolve::script::Source;
use cfsolve::stage2::call_stack;
use cfsolve::stage2::replay::Answers;
use url::Url;

fn main() -> Result<()> {
    let arguments: Vec<String> = env::args().skip(1).collect();
    let [directory, plain, output] = arguments.as_slice() else {
        bail!("usage: capture_answers <session-directory> <plain-payload.json> <output.json>");
    };
    let directory = Path::new(directory);
    // The capture keeps the iframe document; the session pipeline keeps the inline script alone.
    let javascript = match fs::read_to_string(directory.join("iframe.js")) {
        Ok(text) => text,
        Err(_) => {
            let html = match fs::read_to_string(directory.join("iframe.html")) {
                Ok(html) => html,
                Err(_) => {
                    let mut documents: Vec<_> = fs::read_dir(directory)
                        .context("listing the capture directory")?
                        .filter_map(|entry| entry.ok())
                        .map(|entry| entry.path())
                        .filter(|path| {
                            path.extension().is_some_and(|extension| extension == "html")
                        })
                        .collect();
                    documents.sort();
                    let document = documents
                        .into_iter()
                        .next()
                        .context("the capture holds no document")?;
                    fs::read_to_string(document).context("reading the captured document")?
                }
            };
            cfsolve::challenge::inline_javascript(&html).context("the capture holds no script")?
        }
    };
    let (deobfuscated, _) = call_stack::deobfuscate_with_cases(javascript)?;
    let source = Source::try_new(&deobfuscated.text).context("parsing the deobfuscated iframe")?;
    let program = match fs::read_to_string(directory.join("response.decoded.txt")) {
        Ok(encoded) => STANDARD
            .decode(encoded.trim())
            .context("decoding the response program")?,
        Err(_) => {
            let body = fs::read(directory.join("post-body.bin"))
                .context("reading post-body.bin or response.decoded.txt")?;
            let url = fs::read_to_string(directory.join("post-url.txt"))
                .context("reading post-url.txt")?;
            let url = Url::parse(url.trim()).context("parsing the captured POST url")?;
            decode_response(&body, ray(&url)?).context("decoding the captured POST response")?
        }
    };
    let text = fs::read_to_string(plain).context("reading the plaintext payload")?;
    let payload: Payload = serde_json::from_str(&text).context("parsing the plaintext payload")?;
    let answers = Answers::capture(&source, &program, &payload)?;
    fs::write(output, serde_json::to_string_pretty(&answers)?)?;
    println!("wrote {} records to {output}", answers.probes.len());
    Ok(())
}
