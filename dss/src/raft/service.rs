labrpc::service! {
    service raft {
        rpc request_vote(RequestVoteArgs) returns (RequestVoteReply);
        rpc append_entries(AppendEntryArgs) returns (AppendEntryReply);
        rpc install_snapshot(InstallSnapshotArgs) returns (InstallSnapshotReply);

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
    pub command_index: u64,

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

#[derive(Clone, PartialEq, Message)]
pub struct InstallSnapshotReply {
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

#[derive(Clone, PartialEq, Message)]
pub struct Snapshot {
    #[prost(uint64, tag="1")]
    pub last_log_index : u64,

    #[prost(uint64, tag="2")]
    pub last_log_term : u64,

    #[prost(uint64, tag="3")]
    pub last_command_index : u64,

    #[prost(bytes, tag="4")]
    pub data : Vec<u8>,
}


#[derive(Clone, PartialEq, Message)]
pub struct InstallSnapshotArgs {
    #[prost(uint64, tag="1")]
    pub term : u64,

    #[prost(uint64, tag="2")]
    pub from : u64,

    #[prost(uint64, tag="3")]
    pub to : u64,

    #[prost(required, message, tag="4")]
    pub snap : Snapshot,

    #[prost(enumeration="MessageType")]
    pub msg_type: i32
}

#[derive(Clone)]
pub enum RaftMessage {
    MsgRequestVoteReply(RequestVoteReply),
    MsgRequestVoteArgs(RequestVoteArgs),
    MsgAppendEntryArgs(AppendEntryArgs),
    MsgAppendEntryReply(AppendEntryReply),
    MsgPropose(Vec<u8>),
    MsgProposeReply(ProposeReply),
    MsgInstallSnapshotArgs(InstallSnapshotArgs),
    MsgInstallSnapshotReply(InstallSnapshotReply),
    MsgAdvanceSnapshot(Snapshot),
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
    InstallSnapshot = 9,
    InstallSnapshotReply = 10,
}

//pub trait RpcType {
//    type RpcMessageType : Clone;
//    fn get_msg_type(&self) -> MessageType;
//}
//
//impl RpcType for RequestVoteReply {
//    type RpcMessageType = RaftMessage::MsgRequestVoteReply;
//    fn get_msg_type(&self) -> MessageType {
//        return MessageType::RequestVoteReply;
//    }
//}
//
//impl RpcType for RequestVoteArgs {
//    type RpcMessageType = RaftMessage::MsgRequestVoteArgs;
//    fn get_msg_type(&self) -> MessageType {
//       return MessageType::RequestVote;
//    }
//}
//
//impl RpcType for AppendEntryArgs {
//    type RpcMessageType = RaftMessage::MsgAppendEntryArgs;
//    fn get_msg_type(&self) -> MessageType {
//        return MessageType::AppendEntry;
//    }
//}
//
//impl RpcType for AppendEntryReply {
//    type RpcMessageType = RaftMessage::MsgAppendEntryReply;
//    fn get_msg_type(&self) -> MessageType {
//        return MessageType::AppendEntryReply;
//    }
//}
//
//impl RpcType for InstallSnapshotArgs {
//    type RpcMessageType = RaftMessage::MsgInstallSnapshotArgs;
//    fn get_msg_type(&self) -> MessageType {
//        return MessageType::InstallSnapshot;
//    }
//}
//
//impl RpcType for InstallSnapshotReply {
//    type RpcMessageType = RaftMessage::MsgInstallSnapshotReply;
//    fn get_msg_type(&self) -> MessageType {
//        return MessageType::InstallSnapshotReply;
//    }
//}

