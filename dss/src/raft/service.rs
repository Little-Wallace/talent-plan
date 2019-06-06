labrpc::service! {
    service raft {
        rpc request_vote(RequestVoteArgs) returns (RequestVoteReply);
        rpc append_entries(AppendEntryArgs) returns (AppendEntryReply);

        // Your code here if more rpc desired.
        // rpc xxx(yyy) returns (zzz)
    }
}

pub use self::raft::{
    add_service as add_raft_service, Client as RaftClient, Service as RaftService,
};


#[derive(Clone, PartialEq, Message)]
pub struct Entry {
    #[prost(uint64, tag="1")]
    pub term : u64,

    #[prost(uint64, tag="2")]
    pub index : u64,

    #[prost(uint64, tag="3")]
    pub data_index : u64,

    #[prost(bool, tag="4")]
    pub valid : bool,

    #[prost(bytes, tag="5")]
    pub data : Vec<u8>
}

/// Example RequestVote RPC arguments structure.
#[derive(Clone, PartialEq, Message)]
pub struct RequestVoteArgs {
    // Your data here (2A, 2B).
    #[prost(uint64, tag="1")]
    pub term : u64,

    #[prost(uint64, tag="2")]
    pub from : u64,

    #[prost(uint64, tag="3")]
    pub to : u64,

    #[prost(uint64, tag="4")]
    pub last_log_index : u64,

    #[prost(uint64, tag="5")]
    pub last_log_term : u64,

    #[prost(enumeration="MessageType")]
    pub msg_type: i32

}

// Example RequestVote RPC reply structure.
#[derive(Clone, PartialEq, Message)]
pub struct RequestVoteReply {
    // Your data here (2A).
    #[prost(uint64, tag="1")]
    pub term : u64,

    #[prost(uint64, tag="2")]
    pub to : u64,

    #[prost(bool, tag="3")]
    pub accept : bool,

    #[prost(enumeration="MessageType")]
    pub msg_type: i32
}


#[derive(Clone, PartialEq, Message)]
pub struct AppendEntryArgs {
    #[prost(uint64, tag="1")]
    pub term : u64,

    #[prost(uint64, tag="2")]
    pub from : u64,

    #[prost(uint64, tag="3")]
    pub to : u64,

    #[prost(uint64, tag="4")]
    pub prev_log_index : u64,

    #[prost(uint64, tag="5")]
    pub prev_log_term : u64,

    #[prost(uint64, tag="6")]
    pub committed : u64,

    #[prost(message, repeated, tag="7")]
    pub entries : Vec<Entry>,

    #[prost(enumeration="MessageType")]
    pub msg_type: i32
}

#[derive(Clone, PartialEq, Message)]
pub struct AppendEntryReply {
    #[prost(uint64, tag="1")]
    pub term : u64,

    #[prost(uint64, tag="2")]
    pub to : u64,

    #[prost(bool, tag="3")]
    pub accept : bool,

    #[prost(uint64, tag="4")]
    pub last_matched_index : u64,

    #[prost(enumeration="MessageType")]
    pub msg_type: i32
}


#[derive(Clone, Message)]
pub struct HardState {

    #[prost(int32, tag="1")]
    pub vote : i32,

    #[prost(uint64, tag="2")]
    pub term : u64,

    #[prost(uint64, tag="3")]
    pub committed : u64,

    #[prost(message, repeated, tag="4")]
    pub entries : Vec<Entry>,

}

#[derive(Clone, Message)]
pub struct ProposeReply {

    #[prost(uint64, tag="1")]
    pub index : u64,

    #[prost(uint64, tag="2")]
    pub term : u64
}

pub enum RaftMessage {
    MsgRequestVoteReply(RequestVoteReply),
    MsgRequestVoteArgs(RequestVoteArgs),
    MsgAppendEntryArgs(AppendEntryArgs),
    MsgAppendEntryReply(AppendEntryReply),
    MsgPropose(Vec<u8>),
    MsgProposeReply(ProposeReply),
    MsgRaftTick,
    MsgStop
}

// use futures::sync::mpsc::Sender;
use futures::sync::oneshot::Sender;

pub struct RaftRequest {
    pub msg : RaftMessage,
    pub tx : Option<Sender<RaftMessage>>
}


#[derive(Clone, Copy, Debug, PartialEq, Eq, ::prost::Enumeration)]
#[repr(i32)]
pub enum MessageType {
    Unknown = 0,
    RequestVoteReply = 1,
    RequestVote = 2,
    PreRequestReply = 3,
    PreRequestVote = 4,
    AppendEntry = 5,
    AppendEntryReply = 6,
    Heartbeat = 7,
    HeartbeatReply = 8,
}

fn get_msg_type(msg_type : i32) -> MessageType {
    match msg_type {
        0 => MessageType::Unknown,
        1 => MessageType::RequestVoteReply,
        2 => MessageType::RequestVote,
        3 => MessageType::PreRequestReply,
        4 => MessageType::PreRequestVote,
        5 => MessageType::AppendEntry,
        6 => MessageType::AppendEntryReply,
        7 => MessageType::Heartbeat,
        8 => MessageType::HeartbeatReply,
        _ => panic!("error type")
    }
}

impl RequestVoteReply {
    pub fn new (term_ : u64, to_: u64, accept_: bool, t : MessageType)->RequestVoteReply {
        RequestVoteReply {
            term : term_,
            to : to_,
            accept : accept_,
            msg_type : t as i32
        }
    }
    pub fn get_msg_type(&self) -> MessageType {
        return MessageType::from_i32(self.msg_type).unwrap();
    }
}

impl RequestVoteArgs {
   pub fn get_msg_type(&self) -> MessageType {
       return MessageType::from_i32(self.msg_type).unwrap();
   }
}

impl AppendEntryArgs {
    pub fn get_msg_type(&self) -> MessageType {
        return MessageType::from_i32(self.msg_type).unwrap();
    }
}

//#[derive(Clone, Debug, PartialEq, Eq)]
//pub enum RaftError {
//    Timeout = 0,
//    NoLeader = 1,
//    Stopped = 2,
//    Unimplemented(String),
//    Other(String),
//}
//
//use std::{error, fmt};
//
//impl fmt::Display for RaftError {
//    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//        write!(f, "{:?}", self)
//    }
//}
//
//impl error::Error for RaftError {
//    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
//        match *self {
//            RaftError::Timeout => Some()
//            _ => None,
//        }
//    }
//}
