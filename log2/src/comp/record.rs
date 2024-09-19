#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProduceRequest {
    #[prost(message, optional, tag = "1")]
    pub record: ::core::option::Option<Record>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProduceResponse {
    #[prost(uint64, tag = "1")]
    pub offset: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConsumeRequest {
    #[prost(uint64, tag = "1")]
    pub offset: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConsumeResponse {
    #[prost(message, optional, tag = "2")]
    pub record: ::core::option::Option<Record>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Record {
    #[prost(bytes = "vec", tag = "1")]
    pub value: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, tag = "2")]
    pub offset: u64,
}
