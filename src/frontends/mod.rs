//! Frontends are replaceable consumers of the shared engine boundary.

#[cfg(feature = "cli")]
pub mod cli;
#[cfg(feature = "web")]
pub mod web;

use crate::engine::EngineHandle;

/// Common lifecycle for an interactive or batch frontend.
///
/// The trait intentionally says nothing about terminals, async runtimes, HTTP,
/// or rendering. Implementations communicate only through `EngineHandle`.
pub trait Frontend {
    type Error;

    fn run(self, engine: EngineHandle) -> Result<(), Self::Error>;
}
