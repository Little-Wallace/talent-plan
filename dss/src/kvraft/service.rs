labrpc::service! {
    service kv {
        rpc get(GetRequest) returns (GetReply);
        rpc put_append(PutAppendRequest) returns (PutAppendReply);
    }
}
pub use self::kv::{add_service as add_kv_service, Client as KvClient, Service as KvService};

/// Put or Append
#[derive(Clone, PartialEq, Message)]
pub struct PutAppendRequest {
    #[prost(string, tag = "1")]
    pub key: String,
    #[prost(string, tag = "2")]
    pub value: String,
    // "Put" or "Append"
    #[prost(enumeration = "Operator", tag = "3")]
    pub op: i32,

    #[prost(uint64, tag = "4")]
    pub req_id: u64,
    // You'll have to add definitions here.
}

#[derive(Clone, PartialEq, Message)]
pub struct PutAppendReply {
    #[prost(bool, tag = "1")]
    pub wrong_leader: bool,
    #[prost(string, tag = "2")]
    pub err: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct GetRequest {
    #[prost(string, tag = "1")]
    pub key: String,
    // You'll have to add definitions here.
}

#[derive(Clone, PartialEq, Message)]
pub struct GetReply {
    #[prost(bool, tag = "1")]
    pub wrong_leader: bool,
    #[prost(string, tag = "2")]
    pub err: String,
    #[prost(string, tag = "3")]
    pub value: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Enumeration)]
pub enum Operator {
    Unknown = 0,
    Put = 1,
    Append = 2,
}

#[derive(Clone, Message)]
pub struct KVSnapshot {
    #[prost(string, repeated, tag = "1")]
    pub kvs: Vec<String>,

    #[prost(uint64, repeated, tag = "2")]
    pub operators: Vec<u64>,
}


