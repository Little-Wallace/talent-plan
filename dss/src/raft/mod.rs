use std::sync::{Arc, Mutex};

use futures::sync::mpsc::{unbounded, UnboundedSender};
use futures::sync::oneshot::Sender as OneshotSender;
use futures::Future;
use futures::Stream;
use futures::oneshot;
use std::time::Duration;
use labcodec;
use labrpc::RpcFuture;
use labrpc::Error as RpcError;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
pub mod service;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use self::service::*;

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
}

#[derive(Clone, PartialEq)]
pub enum Role {
    Leader = 0,
    Follower = 1,
    Candidate = 2,
    PreCandidate = 3
}


impl Default for Role {
    fn default() -> Role {
        Role::Follower
    }
}
/// State of a raft peer.
#[derive(Default, Clone)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
    pub leader : i32,
    pub role: Role,
}

#[derive(Default, Clone, Debug)]
pub struct RaftLog {
    pub entries : Vec<Entry>
}

const TICK_INTERVAL : i32 = 40;

impl RaftLog {

    pub fn get_entries(&self, since : usize) -> Vec<Entry> {
        if since >= self.entries.len() {
            return Vec::new();
        }
        return self.entries[since..].to_vec();
    }

    pub fn get_term(&self, index : usize) -> Option<u64> {
        if index < self.entries.len() {
            Some(self.entries[index].term)
        } else {
            None
        }
    }

    pub fn get_last_index_term(&self)-> (u64, u64) {
        let entry = self.entries.last().unwrap();
        (entry.index, entry.term)
    }
    pub fn append(&mut self, data : Vec<u8>, term : u64, valid : bool) -> u64 {
        let (index, mut data_index) = match self.entries.last() {
            Some(entry) => (entry.index, entry.data_index),
            None => (0, 0)
        };
        if valid {
            data_index = data_index + 1;
        }
        self.entries.push(Entry { term, index, data_index, valid, data});
        data_index
    }

    pub fn append_entry(&mut self, entry : Entry) {
        let index = entry.index as usize;
        if index >= self.entries.len() {
            self.entries.push(entry);
        } else {
            self.entries[index] = entry;
        }
    }


    pub fn match_index_term(&self, index : usize, term : u64) -> bool {
        if index >= self.entries.len() {
            return false;
        } else {
            return self.entries[index].term == term;
        }
    }
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

#[derive(Default, Clone)]
pub struct Peer {
    active : bool,
    next_index : u64,
    match_index : i64,
}

use std::time;
// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    clients: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    committed : u64,
    vote: i32,
    voters: Vec<i32>,
    peers : Vec<Peer>,
    state: Arc<Mutex<State>>,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    log : RaftLog,
    last_heartbeat_time : time::Instant,
    last_bcast_time : time::Instant,
    election_timeout : u64,
    pub sender : Option<UnboundedSender<RaftRequest>>,
    pub worker : Option<CpuPool>,
    pub prev_hard_state : Option<HardState>,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();
        let sz = peers.len();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            persister,
            me,
            committed : 0,
            vote: -1,
            clients : peers,
            peers : vec![Peer::default(); sz],
            voters : vec![0; sz],
            state: Arc::default(),
            log : RaftLog::default(),
            last_heartbeat_time : time::Instant::now(),
            last_bcast_time : time::Instant::now(),
            election_timeout : 800 + 40 * me as u64,
            sender : Option::None,
            worker : Option::None,
            prev_hard_state : Option::None,
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        println!("start raft node {}.", me);
        rf
    }

    fn maybe_persist(&mut self) {
        let state = self.get_state();
        match self.prev_hard_state.as_mut() {
            Some(hard_state) => {
                if hard_state.term != state.term || hard_state.vote != self.vote || hard_state.committed != self.committed {
                    hard_state.term = state.term;
                    self.persist();
                    return;
                }
                match hard_state.entries.last() {
                    Some(entry) => {
                        let (index, term) = self.log.get_last_index_term();
                        if entry.index != index || entry.term != term {
                            // maybe_append(&mut hard_state.entries, &self.log.entries);
                            hard_state.entries = self.log.entries.clone();
                            self.persist();
                        }
                    },
                    None => {
                        if self.log.entries.len() > 0 {
                            hard_state.entries = self.log.entries.clone();
                            self.persist();
                        }
                    }
                }
            }
            None => {
                let hard_state = HardState {
                    term : state.term,
                    committed : self.committed,
                    vote : self.vote,
                    entries : self.log.entries.clone(),
                };
                self.prev_hard_state = Some(hard_state);
                self.persist();
            }
        };
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
        let mut data = Vec::new();
        labcodec::encode(self.prev_hard_state.as_ref().unwrap(), &mut data).unwrap();
        self.persister.save_raft_state(data);
    }



    fn tick(&mut self) {
        // println!("{} tick ", self.me);
        let state = self.get_state();
        if state.is_leader {
            if self.last_bcast_time.elapsed() > time::Duration::from_millis(200) {
                self.bcast_heartbeat(&state);
                self.last_bcast_time = time::Instant::now();
            }
            if self.last_heartbeat_time.elapsed() > time::Duration::from_millis(800) {
                self.check_leader(state);
                self.last_heartbeat_time = time::Instant::now();
            }
        } else {
            if self.last_heartbeat_time.elapsed() > time::Duration::from_millis(self.election_timeout) {
                self.campaign(MessageType::PreRequestVote, &state);
                self.last_heartbeat_time = time::Instant::now();
            }
        }
    }

    fn check_leader(&mut self, state : State) {
        let mut active = 0;
        for i in 0..self.peers.len() {
            if i == self.me {
                active += 1;
            } else if self.peers[i].active {
                active += 1;
                self.peers[i].active = false;
            }
        }
        if active <= self.peers.len() / 2 {
            self.become_role(-1, state.term, Role::Follower);
        }
    }

    fn send_append_entry(&mut self, server: usize, state: &State) {
        let entries = self.log.get_entries(self.peers[server].next_index as usize).clone();
        let prev_log_index = self.peers[server].next_index - 1;
        let prev_log_term = self.log.get_term(prev_log_index as usize).unwrap();
        let args = AppendEntryArgs {
            term : state.term,
            from : self.me as u64,
            to : server as u64,
            msg_type : MessageType::AppendEntry as i32,
            prev_log_index,
            prev_log_term,
            entries,
        };
        let sender = self.sender.clone();
        let f = self.clients[server].append_entries(&args).map(move |res| {
            let ret = sender.unwrap().unbounded_send(RaftRequest { msg : RaftMessage::MsgAppendEntryReply(res), tx : Option::None })
                .map_err(|e| {
                    println!("send append reply error");
                });
        }).map_err(move |e| {
            println!("send append entry to {} failed, {:?} ", server, e);
        });
        let tmp = self.worker.as_ref();
        match tmp {
            Some(worker) => worker.spawn(f).forget(),
            None => panic!("No worker"),
        };
        println!("start send append entry to {}", server);
    }
    fn send_heartbeat(&mut self, server: usize, state: &State) {
        let args = AppendEntryArgs {
            term : state.term,
            from : self.me as u64,
            to : server as u64,
            msg_type : MessageType::Heartbeat as i32,
            prev_log_index : 0,
            prev_log_term : 0,
            entries : Vec::new(),
        };
        let sender = self.sender.clone();
        let f = self.clients[server].append_entries(&args).map(move |res| {
            let ret = sender.unwrap().unbounded_send(RaftRequest { msg : RaftMessage::MsgAppendEntryReply(res), tx : Option::None })
                .map_err(|e| {
                println!("send append reply error");
            });
        }).map_err(move |e| {
            println!("send append entry to {} failed, {:?} ", server, e);
        });
        let tmp = self.worker.as_ref();
        match tmp {
            Some(worker) => worker.spawn(f).forget(),
            None => panic!("No worker"),
        };
        // println!("start send append entry to {}", server);
    }

    fn bcast_heartbeat(&mut self, state: &State) {
        for i in 0..self.clients.len() {
            if i != self.me {
                self.send_heartbeat(i, state);
            }
        }
        println!("{} bcast heartbeat", state.leader);
    }

    fn bcast_append(&mut self, state: &State) {
        for i in 0..self.clients.len() {
            if i != self.me {
                self.send_append_entry(i, state);
            }
        }
    }
    fn timestamp(&self) -> u128 {
        use time::SystemTime;
        SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis()
    }
    fn campaign(&mut self, msg_type : MessageType, state : &State) {
        // let campaign_begin = time::Instant::now();
        let campaign_begin = time::Instant::now();
        let c_begin = self.timestamp();
        println!("{} begin {} campagin at {}", self.me, msg_type as i32, c_begin);
        let mut term = state.term.clone();
        if msg_type == MessageType::PreRequestVote {
            self.become_role(-1, term, Role::PreCandidate);
        } else {
            self.become_role(-1, term + 1, Role::Candidate);
        }
        term = term + 1;
        let (last_index, last_term) = self.log.get_last_index_term();
        let mut args = RequestVoteArgs {
            term,
            msg_type : msg_type as i32,
            to : 0,
            from : self.me as u64,
            last_log_index : last_index,
            last_log_term : last_term
        };
        self.vote = self.me as i32;
        println!("{} voters: {}", self.me, self.voters.len());
        self.voters[self.me] = 1;
        for i in 0..self.peers.len() {
            if i == self.me {
                continue
            }
            args.to = i as u64;
            self.send_request_vote(i, args.clone());
        }
    }

    fn become_role(&mut self, leader : i32, term : u64, role: Role) -> State {
        let r = role.clone();
        let state = State {
            role,
            term,
            leader,
            is_leader : leader == self.me as i32,
        };

        *self.state.lock().unwrap() = state.clone();
        self.vote = -1;
        self.last_heartbeat_time = time::Instant::now();
        self.last_bcast_time = time::Instant::now();
        for i in 0..self.voters.len() {
            self.voters[i] = 0;
            if r == Role::Leader {
                self.peers[i].match_index = 0;
                self.peers[i].next_index = 1;           // every raft instance has the same first entry
            }
        }
        state
    }

    fn get_reply_type_for_request(&self, msg_type : MessageType) -> MessageType{
        match msg_type {
            MessageType::RequestVote  => MessageType::RequestVoteReply,
            MessageType::PreRequestVote => MessageType::PreRequestReply,
            MessageType::AppendEntry => MessageType::AppendEntryReply,
            MessageType::Heartbeat => MessageType::HeartbeatReply,
            _ => panic!("not a valid message type"),
        }
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            let s = self.state.clone();
            let mut state = s.lock().unwrap();
            self.log.append(Vec::new(), 0, false);
            *state = State {
                term : 0,
                is_leader : false,
                role : Role::Follower,
                leader : -1,
            };
            return;
        }

        match labcodec::decode(data) {
            Ok(recover_state) => {
                self.recover(&recover_state);
                self.prev_hard_state = Some(recover_state);
            },
            Err(e) => {
                panic!("restore error {:?}", e);
            }
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }


    fn recover(&mut self, data: &HardState) {
        let s = self.state.clone();
        let mut state = s.lock().unwrap();
        state.term = data.term;
        state.role = Role::Follower;
        state.is_leader = false;
        state.leader = -1;
        self.committed = data.committed;
        self.vote = data.vote;
        self.log.entries = data.entries.clone();
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/mod.rs for more details.
    fn send_request_vote(&mut self, server: usize, args: RequestVoteArgs) {
        let client = &self.clients[server];
        let sender = self.sender.clone();
        let rpc_cost = time::Instant::now();
        let f = client.request_vote(&args).map(move |res| {
            println!("{} to {} request vote rpc run {}", args.from, args.to, rpc_cost.elapsed().as_millis());
            let ret = sender.unwrap().unbounded_send(
                RaftRequest { msg : RaftMessage::MsgRequestVoteReply(res), tx : Option::None }).map_err(|error| {
                println!("send error: {:?}", error);
            });
        }).map_err(move |e| {
            println!("send request vote to {} failed, {:?} ", server, e);
        });
        let tmp = self.worker.as_ref();
        match tmp {
            // Some(worker) => worker.spawn(f).forget(),
            Some(worker) => worker.execute(f).unwrap(),
            None => panic!("No worker"),
        };
//        let sender2 = self.sender.clone();
//        sender2.unwrap().unbounded_send(RaftRequest { msg : RaftMessage::MsgRaftTick, tx : Option::None}).map_err(|error| {
//            println!("{} test send failed {:?}", self.me, error);
//        }).unwrap();
//
        println!("{} start send request vote to {}", self.me, server);

        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        // let (tx, rx) = channel();
        // peer.spawn(
        //     peer.request_vote(&args)
        //         .map_err(Error::Rpc)
        //         .then(move |res| {
        //             tx.send(res);
        //             Ok(())
        //         }),
        // );
        // rx.wait() ...
        // ```
    }

    fn handle_request_vote(&mut self, args : RequestVoteArgs, sender : OneshotSender<RaftMessage>) {
        let state = self.get_state();
        let msg_type = self.get_reply_type_for_request(args.get_msg_type());
        let mut reply = RequestVoteReply {
            term : state.term,
            to : self.me as u64,
            accept : false,
            msg_type : msg_type as i32,
        };
        println!("{}(term {}) receive RequestVote({}) for {}(term: {}) at {}",
                 self.me, state.term, args.msg_type, args.from, args.term, self.timestamp());
        let me = self.me;
        let (index, term) = self.log.get_last_index_term();
        let in_lease = state.leader != -1 && self.last_heartbeat_time.elapsed() < Duration::from_millis(700);
        let allow_vote = self.vote == -1 || self.vote == args.from as i32;
        if state.term > args.term {
            reply.accept = false;
        } else if index > args.last_log_index || (index == args.last_log_index && term > args.term) {
            reply.accept = false;
        } else if args.term > state.term {
            if !in_lease {
                reply.accept = true;
                println!("{} has access request vote {} for {} because no lease {}", self.me, args.msg_type, args.from, state.leader);
                if args.get_msg_type() == MessageType::RequestVote {
                    self.become_role(-1, args.term, Role::Follower);
                }
            }
        } else {
            match state.role {
                Role::Follower => if state.leader == -1 && allow_vote{
                    reply.accept = true;
                    println!("{} has access request vote {} for {}", self.me, args.msg_type, args.from);
                },
                Role::Candidate | Role::PreCandidate => if allow_vote {
                    reply.accept = true;
                    println!("{} has access request vote {} for {}", self.me, args.msg_type, args.from);
                },
                Role::Leader =>  {
                    reply.accept = false;
                }
            }
        }
        if reply.accept {
            if args.get_msg_type() == MessageType::RequestVote {
                self.last_heartbeat_time = time::Instant::now();
                // self.state.lock().unwrap().term = args.term;
                reply.term = args.term;
                self.vote = args.from as i32;
            }
        }
        sender.send(RaftMessage::MsgRequestVoteReply(reply)).map_err(|e| {
            println!("send vote reply error");
        }).unwrap();
        // sender.send(RaftMessage::MsgRequestVoteReply(reply)).unwrap_err();
    }

    fn calculate_vote(&mut self, server : usize, accept : bool) -> i32 {
        if accept {
            self.voters[server] = 1;
        } else {
            self.voters[server] = -1;
        }
        let mut win = 0;
        let mut lose = 0;
        for i in 0..self.voters.len() {
            if self.voters[i] == 1 {
                win += 1;
            } else if self.voters[i] == -1 {
                lose += 1;
            }
        }
        if win > self.voters.len() / 2 {
            return 1;
        }
        if lose >= (self.voters.len() + 1) / 2 {
            return -1;
        }
        return 0;
    }

    fn handle_request_vote_reply(&mut self, reply : RequestVoteReply) {
        let state = self.get_state();
        println!("{} (role: {}, term: {}) receive a request_vote_reply(type : {}, term: {}) from {}, which accept: {} at {}",
                 self.me, state.role.clone() as i32, state.term, reply.msg_type, reply.term, reply.to, reply.accept, self.timestamp());

        if reply.term < state.term {
            return;
        }
        match state.role {
            Role::Leader | Role::Follower => {
                return;
            },
            Role::Candidate => {
                if reply.get_msg_type() != MessageType::RequestVoteReply {
                    return;
                }
                let ret = self.calculate_vote(reply.to as usize, reply.accept);
                if ret == 1{
                    println!("{} win the election in term {}, and become leader", self.me, state.term);
                    let new_state = self.become_role(self.me as i32, state.term, Role::Leader);
                    self.propose(Vec::new(), false, &new_state);
                } else if ret == -1 {
                    self.become_role(-1, state.term,Role::Follower);
                }
            },
            Role::PreCandidate => {
                if reply.get_msg_type() != MessageType::PreRequestReply {
                    return;
                }

                let ret = self.calculate_vote(reply.to as usize, reply.accept);
                if ret == 1{
                    self.campaign(MessageType::RequestVote, &state);
                } else if ret == -1 {
                    self.become_role(-1, state.term,Role::Follower);
                    println!("{} win the campaign", self.me);
                } else {
                    return;
                }
            }
        }
    }

    fn handle_append_entries(&mut self, args : AppendEntryArgs, sender : OneshotSender<RaftMessage>) {
        let state = self.get_state();
        let msg_type = MessageType::from_i32(args.msg_type).unwrap();
        let reply_msg_type = self.get_reply_type_for_request(args.get_msg_type());
        let mut reply = AppendEntryReply {
            term : state.term,
            to : self.me as u64,
            accept : false,
            last_matched_index : 0,
            msg_type : reply_msg_type as i32,
        };
        if state.term > args.term {
            reply.accept = false;
            reply.term = state.term;
            reply.last_matched_index = 0;
            return;
        }
        if state.term < args.term || state.role != Role::Follower || state.leader != args.from as i32 {
            self.become_role(args.from as i32, args.term, Role::Follower);
            println!("{} become follower of {} at term {}, {}", self.me, args.from, args.term, self.timestamp());
        }
        self.last_heartbeat_time = time::Instant::now();
        if msg_type == MessageType::Heartbeat {
            reply.accept = true;
        } else if msg_type == MessageType::AppendEntry {
            let (index, term) = self.log.get_last_index_term();
            if self.log.match_index_term(args.prev_log_index as usize, args.prev_log_term) {
                for entry in &args.entries {
                    self.log.append_entry(entry.clone());
                }
                match args.entries.last() {
                    Some(last) => reply.last_matched_index = last.index,
                    None => reply.last_matched_index = args.prev_log_index,
                }
                reply.accept = true;
            } else {
                reply.accept = false;
                reply.last_matched_index = args.prev_log_index - 1;
                if args.prev_log_index > 1 + index {
                    reply.last_matched_index = index;
                }
            }
        }
        sender.send(RaftMessage::MsgAppendEntryReply(reply)).map_err(|e| {
            println!("send append reply error");
        }).unwrap();

    }

    fn handle_append_entries_reply(&mut self, reply : AppendEntryReply) {
        let state = self.get_state();
        if reply.term > state.term {
            self.become_role(-1, reply.term, Role::Follower);
            return;
        }
        let msg_type = MessageType::from_i32(reply.msg_type).unwrap();
        let to = reply.to as usize;
        let mut peer = &mut self.peers[to];
        peer.active = true;
        if msg_type == MessageType::AppendEntryReply {
            if reply.accept {
                peer.match_index = reply.last_matched_index as i64;
            }
        }
    }

    fn propose(&mut self, data : Vec<u8>, valid : bool, state: &State) -> u64 {
        let data_index = self.log.append(data, state.term, valid);
        println!("{} propose entry in term {}", state.leader, state.term);
        self.bcast_append(state);
        data_index
    }

    pub fn step(&mut self, req : RaftRequest) {
        match req.msg {
            RaftMessage::MsgRequestVoteArgs(args) => self.handle_request_vote(args, req.tx.unwrap()),
            RaftMessage::MsgRequestVoteReply(reply) => self.handle_request_vote_reply(reply),
            RaftMessage::MsgAppendEntryArgs(args) => self.handle_append_entries(args, req.tx.unwrap()),
            RaftMessage::MsgAppendEntryReply(reply) => self.handle_append_entries_reply(reply),
            RaftMessage::MsgRaftTick => self.tick(),
        }
        // self.maybe_persist();
    }

    pub fn get_state(&self) -> State {
        return self.state.lock().unwrap().clone();
    }

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```

use futures_cpupool::CpuPool;
use futures::future::Executor;

#[derive(Clone)]
pub struct Node {
    // Your code here.
    me : usize,
    sender : UnboundedSender<RaftRequest>,
    worker : CpuPool,
    state: Arc<Mutex<State>>,
    stop : Arc<Mutex<bool>>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        let (sender, receiver) = unbounded();
        let dur = Duration::from_millis(40);
        let sender_tick = sender.clone();
        let me = raft.me;
        let mut instance = raft;
        let worker = CpuPool::new(1);
        instance.sender = Some(sender.clone());
        instance.worker = Some(worker.clone());
        println!("raft voters : {}.", instance.voters.len());
        let state = instance.state.clone();
        let stream =
            receiver.for_each(move | req: RaftRequest| {
                instance.step(req);
                Ok(())
            }).map_err(move |e| {
                println!("raft step stopped: {:?}", e)
            });
        use futures_timer::Interval;
        let stream2 = Interval::new(dur)
            .for_each(move | ()| {
                sender_tick.unbounded_send(RaftRequest {
                    msg : RaftMessage::MsgRaftTick,
                    tx : Option::None
                }).map_err(|_| {
                    println!("tick failed");
                }).unwrap();
                // println!("{} tick raft", me);
                Ok(())
            })
            .map_err(move |e| debug!("raft tick stopped: {:?}",  e));

        //use futures::IntoFuture;
        worker.spawn(stream).forget();
        // let fs = worker.spawn(stream);
        worker.spawn(stream2).forget();
        Node {
            me,
            sender : sender.clone(),
            worker : worker.clone(),
            state : state,
            stop : Arc::new(Mutex::new(false)),
        }
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns false. otherwise start the
    /// agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first return value is the index that the command will appear at
    /// if it's ever committed. the second return value is the current
    /// term. the third return value is true if this server believes it is
    /// the leader.
    /// This method must return quickly.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        unimplemented!()
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        // unimplemented!()
        self.get_state().term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        // unimplemented!()
        self.get_state().is_leader
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        self.state.lock().unwrap().clone()
    }

    pub fn is_stop(&self) -> bool {
        *self.stop.lock().unwrap()
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
        // self.sender.close();
        *self.stop.lock().unwrap() = true;
        println!("kill raft node {}", self.me);
    }
}

impl RaftService for Node {
    // example RequestVote RPC handler.
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        // Your code here (2A, 2B).
        // self.raft.lock().unwrap().step(RaftRequest { msg : RaftMessage::RequestVoteArgs(args), tx : tx});
        if self.sender.is_closed() {
            println!("{} sender is closed", self.me);
            return Box::new(futures::future::result(Err(RpcError::Stopped)));
        }

        //let rpc_cost = time::Instant::now();

        println!("{} begin request vote rpc", self.me);
        let (tx_, rx_) = oneshot();
        let sender = self.sender.clone();
        // let ret = sender.unbounded_send(RaftRequest { msg : RaftMessage::MsgRequestVoteArgs(args), tx : Some(tx_)}).map_err(|error| {
        //    println!("send to raft thread failed {:?}", error);
        // });

        // if ret.is_err() {
        //     println!("{} send to raft thread error.", self.me);
        // }

        sender.unbounded_send(RaftRequest { msg : RaftMessage::MsgRequestVoteArgs(args), tx : Some(tx_)})
            .map_err(|e| {
                println!("send vote reply error");
            }).unwrap();
        return Box::new(rx_.map( | msg : RaftMessage | {
            // println!("request vote rpc run {}", rpc_cost.elapsed().as_millis());
            match msg {
                RaftMessage::MsgRequestVoteReply(reply) => {
                    return reply;
                }
                _ => {
                    panic!("error type")
                }
            }
        }).map_err(move |e| {
            return RpcError::Other(String::from("request_vote rpc failed, sender cancel"));
        }));
    }

    fn append_entries(&self, args: AppendEntryArgs) -> RpcFuture<AppendEntryReply> {
        if self.sender.is_closed() {
            return Box::new(futures::future::result(Err(RpcError::Stopped)));
        }

        let (tx_, rx_) = oneshot();
        let sender = self.sender.clone();
        sender.unbounded_send(RaftRequest { msg : RaftMessage::MsgAppendEntryArgs(args), tx : Some(tx_)}).map_err(|e| {
            println!("send append reply error");
        }).unwrap();
        // use futures::Stream;
        return Box::new(rx_.map(| msg : RaftMessage | {
            match msg {
                RaftMessage::MsgAppendEntryReply(reply) => {
                    return reply;
                }
                _ => {
                    panic!("error type")
                }
            }
        }).map_err(move |e| {
            return RpcError::Other(String::from("append_entries rpc failed, sender cancel"));
        }));
    }

//    fn test_request_vote(&mut self) {
//        let args = RequestVoteArgs {
//            term : 0,
//            to : 0,
//            from : 0,
//            last_log_term : 0,
//            last_log_index : 0,
//            msg_type : MessageType::PreRequestVote as i32,
//        };
//        self.request_vote(args).map(|reply : RequestVoteReply| {
//            println!("receive a reply {}", reply.to);
//        }).map_err(|e| println!("request vote failed {}", e)).fo;
//    }
}
