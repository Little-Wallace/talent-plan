labrpc::service! {
    service raft {
        rpc request_vote(RequestVoteArgs) returns (RequestVoteReply);

        // Your code here if more rpc desired.
        // rpc xxx(yyy) returns (zzz)
    }
}
pub use self::raft::{
    add_service as add_raft_service, Client as RaftClient, Service as RaftService,
};

/// Example RequestVote RPC arguments structure.
#[derive(Clone, PartialEq, Message)]
pub struct RequestVoteArgs {
    // Your data here (2A, 2B).
    #[prost(int32, tag="1")]
    pub term : i32,

    #[prost(int32, tag="2")]
    pub from : i32,

    #[prost(int32, tag="3")]
    pub to : i32,

    #[prost(int32, tag="4")]
    pub last_log_index : i32,

    #[prost(int32, tag="5")]
    pub last_log_term : i32,

    #[prost(enumeration="MessageType")]
    pub msg_type: i32

}

// Example RequestVote RPC reply structure.
#[derive(Clone, PartialEq, Message)]
pub struct RequestVoteReply {
    // Your data here (2A).
    #[prost(int32, tag="1")]
    pub term : i32,

    #[prost(int32, tag="2")]
    pub to : i32,

    #[prost(bool, tag="3")]
    pub accept : bool,

    #[prost(enumeration="MessageType")]
    pub msg_type: i32
}


pub enum RaftMessage {
    RequestVoteReply(RequestVoteReply),
    RequestVoteArgs(RequestVoteArgs),
}

// use futures::sync::mpsc::Sender;
use futures::sync::oneshot::Sender;

pub struct RaftRequest {
    pub msg : RaftMessage,
    pub tx : Sender<RaftMessage>
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Enumeration)]
#[repr(i32)]
pub enum MessageType {
    Unknown = 0,
    RequestVoteReply = 1,
    RequestVote = 2,
    PreRequestVote = 3,
    PreRequestReply = 4,
}

impl RequestVoteReply {
    pub fn new (term_ : i32, to_: i32, accept_: bool, t : MessageType)->RequestVoteReply {
        RequestVoteReply {
            term : term_,
            to : to_,
            accept : accept_,
            msg_type : t as i32
        }
    }

}

