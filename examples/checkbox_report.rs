//! Reports how a captured checkbox program is recovered, from a captured session.
//!
//! Usage: checkbox_report <session-directory> [post-url.txt] [post-body.bin]
//!
//! The directory holds `iframe.js` or `iframe.html`; by default the program comes from
//! `post-body.bin` + `post-url.txt`, which `capture_answers` writes.

use std::path::Path;
use std::{env, fs};

use anyhow::{Context, Result, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use cfsolve::challenge::{decode_response, ray};
use cfsolve::script::Source;
use cfsolve::stage2::call_stack;
use cfsolve::stage2::interaction;
use url::Url;

fn main() -> Result<()> {
    let arguments: Vec<String> = env::args().skip(1).collect();
    let [directory, rest @ ..] = arguments.as_slice() else {
        bail!("usage: checkbox_report <session-directory> [post-url.txt] [post-body.bin]");
    };
    let directory = Path::new(directory);
    let (url_name, body_name) = match rest {
        [url, body] => (url.as_str(), body.as_str()),
        _ => ("post-url.txt", "post-body.bin"),
    };
    let javascript = match fs::read_to_string(directory.join("iframe.js")) {
        Ok(text) => text,
        Err(_) => {
            let html = fs::read_to_string(directory.join("iframe.html"))
                .context("reading the captured iframe document")?;
            cfsolve::challenge::inline_javascript(&html).context("the capture holds no script")?
        }
    };
    let (deobfuscated, _) = call_stack::deobfuscate_with_cases(javascript)?;
    let source = Source::try_new(&deobfuscated.text).context("parsing the deobfuscated iframe")?;
    let body = fs::read(directory.join(body_name)).context("reading the POST body")?;
    let url = fs::read_to_string(directory.join(url_name)).context("reading the POST url")?;
    let url = Url::parse(url.trim()).context("parsing the POST url")?;
    let decoded = decode_response(&body, ray(&url)?).context("decoding the POST response")?;
    let program = STANDARD.decode(decoded).context("decoding the program")?;
    let machine = source.machine()?;
    let listing = machine.disassemble_from(&program, &[machine.entry()], &["cf-chl-peek"])?;
    println!(
        "{} instructions, {} functions, {} failures",
        listing.instructions.len(),
        listing.functions.len(),
        listing.failures.len()
    );
    let plan = interaction::recover(&listing);
    match plan {
        Ok(_) => println!("recovered the checkbox plan"),
        Err(error) => println!("error: {error:?}"),
    }
    Ok(())
}
