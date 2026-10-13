//! Muse Code as an Agent Client Protocol agent.
//!
//! [`msp`] is a client for the Muse Session Protocol that `muse serve` speaks;
//! [`bridge`] is the ACP agent in front of it. The binary wires the two to
//! stdin, stdout and a `muse serve` child; the tests wire them to in-memory
//! pipes.

pub mod bridge;
pub mod msp;
mod translate;
