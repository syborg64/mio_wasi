#[cfg(feature = "wasmtime")]
mod wasmtime;
#[cfg(feature = "wasmtime")]
pub use self::wasmtime::*;

#[cfg(feature = "wasmedge")]
mod wasmedge;
#[cfg(feature = "wasmedge")]
pub use self::wasmedge::*;

#[cfg(feature = "wamr")]
mod wamr;
#[cfg(feature = "wamr")]
pub use self::wamr::*;

#[cfg(not(any(feature = "wasmedge", feature = "wamr", feature = "wasmtime")))]
compile_error!("You need to enable one of the runtime impl features (wasmedge, wamr, wasmtime) for the net feature!");