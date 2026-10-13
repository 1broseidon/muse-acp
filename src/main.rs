//! `muse-acp`: Muse Code as an Agent Client Protocol agent.
//!
//! An ACP client (Zed, Hotline, anything that speaks ACP) starts this binary
//! and talks to it on stdin and stdout. It starts `muse serve` behind itself
//! and translates between the two protocols, so the person's own Muse login,
//! settings, sandbox and session history are the ones in use.

use muse_acp::{bridge, msp};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

const USAGE: &str = "\
muse-acp: Muse Code over the Agent Client Protocol

Usage:
  muse-acp [--muse <PATH>] [-- <muse serve arguments>...]
  muse-acp [--muse <PATH>] login

With no command, serves ACP on stdin and stdout, backed by `muse serve`.
Arguments after `--` go to `muse serve` as they are, for example
`-- --provider meta --model muse-spark-1.3` or `-- --disable-sandbox`.

`login` runs `muse login`, which an ACP client offers as its sign-in.

Options:
  --muse <PATH>  The muse executable (default: `muse` on PATH)
  -h, --help     Print this help
  -V, --version  Print the version";

#[tokio::main]
async fn main() -> ExitCode {
    let mut muse = PathBuf::from("muse");
    let mut serve_args = Vec::new();
    let mut login = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--muse" => match args.next() {
                Some(path) => muse = PathBuf::from(path),
                None => return usage_error("--muse needs a path"),
            },
            "login" => login = true,
            "--" => serve_args.extend(args.by_ref()),
            "-h" | "--help" => {
                println!("{USAGE}");
                return ExitCode::SUCCESS;
            }
            "-V" | "--version" => {
                println!("muse-acp {}", env!("CARGO_PKG_VERSION"));
                return ExitCode::SUCCESS;
            }
            other => return usage_error(&format!("unexpected argument `{other}`")),
        }
    }
    if login {
        return run_login(&muse);
    }

    let (mut child, msp, events) = match msp::spawn(&muse, &serve_args) {
        Ok(started) => started,
        Err(error) => {
            eprintln!(
                "muse-acp: could not start `{} serve`: {error}. Install Muse Code, or pass --muse <PATH>.",
                muse.display()
            );
            return ExitCode::FAILURE;
        }
    };
    let served = bridge::serve(agent_client_protocol::Stdio::new(), msp, events).await;
    // The bridge has dropped its end of the host's stdin, which is how
    // `muse serve` is told to shut down; it gets a moment to do so cleanly.
    if tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .is_err()
    {
        let _ = child.kill().await;
    }
    match served {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("muse-acp: {error:?}");
            ExitCode::FAILURE
        }
    }
}

/// `muse login`, run in the terminal the client opened for it. The child
/// inherits that terminal, so its browser code and prompts are the person's.
fn run_login(muse: &std::path::Path) -> ExitCode {
    match std::process::Command::new(muse)
        .arg("login")
        .env_remove("META_API_KEY")
        .status()
    {
        Ok(status) if status.success() => ExitCode::SUCCESS,
        Ok(_) => ExitCode::FAILURE,
        Err(error) => {
            eprintln!(
                "muse-acp: could not run `{} login`: {error}",
                muse.display()
            );
            ExitCode::FAILURE
        }
    }
}

fn usage_error(message: &str) -> ExitCode {
    eprintln!("muse-acp: {message}\n\n{USAGE}");
    ExitCode::from(2)
}
