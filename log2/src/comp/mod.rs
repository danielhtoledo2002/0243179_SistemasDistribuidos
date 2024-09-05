pub mod config;
pub mod index;
pub mod segments;
pub mod store;
mod log {
    include!(concat!(env!("OUT_DIR"), "/log.v1.rs"));
}
