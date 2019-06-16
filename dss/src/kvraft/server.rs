use super::service::*;
use crate::raft;

use futures::sync::mpsc::unbounded;
use futures::sync::oneshot::Sender as OneshotSender;
use futures::future::result as FutureResult;
use futures::oneshot;
use labrpc::RpcFuture;
use labrpc::Error as RpcError;
use std::collections::{HashMap, HashSet};
use crate::raft::ApplyMsg;
use crate::raft::ApplyType;
use crate::raft::errors::Error as RaftError;

use futures_cpupool::CpuPool;
use std::sync::{Arc, Mutex};
use futures::stream::Stream;
use futures::future::Future;
use futures_timer::Interval;
use std::time::Duration;
use crate::raft::service::{InstallSnapshotArgs, Snapshot};


#[derive(Default, Clone)]
pub struct ApplyResult {
    value : String,
    term : u64,
}

#[derive(Default)]
pub struct Storage {
    mp : HashMap<String, String>,
    operators : HashSet<u64>,
    callbacks : HashMap<u64, OneshotSender<ApplyResult>>,
    max_apply_index : u64,
    snap : Option<InstallSnapshotArgs>,
}

impl Storage {
    pub fn apply(&mut self, req: PutAppendRequest, index: u64, term : u64, me: usize) {
        let op = Operator::from_i32(req.op).unwrap();
        let key = req.key.clone();
        let value = req.value.clone();
        let ret_value = match op {
            Operator::Unknown => {
                match self.mp.get(&key) {
                    Some(v) => v.clone(),
                    None => String::from("")
                }
            },
            Operator::Put => if !self.operators.contains(&req.req_id) {
                let i_value = value.clone();
                self.mp.insert(key, i_value);
                self.operators.insert(req.req_id);
                let last_req_id = req.req_id - 2;
                self.operators.remove(&last_req_id);
                value
            } else {
                self.mp.get(&req.key).unwrap().clone()
            },
            Operator::Append => if !self.operators.contains(&req.req_id) {
                let f_value = match self.mp.get_mut(&key) {
                    Some(value_mut) => {
                        value_mut.push_str(value.as_str());
                        value_mut.clone()
                    },
                    None => {
                        self.mp.insert(key, value.clone());
                        value
                    },
                };
                self.operators.insert(req.req_id);
                let last_req_id = req.req_id - 2;
                self.operators.remove(&last_req_id);

                f_value
            }
            else {
                self.mp.get(&req.key).unwrap().clone()
            }
        };
        println!("{}, storage apply an entry of index: {}, op: {}, key: {:?}, value: {:?}, ret_value: {:?}",
                 me, index, op as i32, req.key, req.value, ret_value);
        self.max_apply_index = index;
        match self.callbacks.remove(&index) {
            Some(sender) => {
                let ret = ApplyResult {
                    value : ret_value,
                    term
                };
                sender.send(ret).map_err(|e| {
                    println!("send vote reply error");
                }).unwrap();
            },
            None => ()
        }
    }

    pub fn apply_snapshot(&mut self, snap : KVSnapshot) {
        let l = snap.kvs.len() / 2;
        for i in 0..l {
            self.mp.insert(snap.kvs[i * 2].clone(), snap.kvs[i * 2 + 1].clone());
        }
        for k in snap.operators {
            self.operators.insert(k);
        }
    }

    pub fn make_snapshot(&mut self) -> KVSnapshot {
        let mut mp = Vec::new();
        let mut operators = Vec::new();
        for (k, v) in self.mp.iter_mut() {
            mp.push(k.clone());
            mp.push(v.clone());
        }
        for k in &self.operators {
            operators.push(k.clone());
        }

        KVSnapshot {
            kvs: mp,
            operators,
        }
    }


    pub fn add_sender(&mut self, index: u64, sender: OneshotSender<ApplyResult>) {
        self.callbacks.insert(index, sender);
    }

    pub fn clear_callback(&mut self) {
        self.callbacks.clear();
    }
}

pub struct KvServer {
    pub rf: raft::Node,
    me: usize,
    // snapshot if log grows this big
    maxraftstate: Option<usize>,
    // Your definitions here.
    worker : CpuPool,
    storage : Arc<Mutex<Storage>>,
}


impl KvServer {
    pub fn new(
        servers: Vec<raft::service::RaftClient>,
        me: usize,
        persister: Box<dyn raft::persister::Persister>,
        maxraftstate: Option<usize>,
    ) -> KvServer {
        // You may need initialization code here.

        let (tx, apply_ch) = unbounded();
        let storage : Arc<Mutex<Storage>> = Arc::default();
        let mut rf = raft::Raft::new(servers, me, persister, tx);
        rf.maxraftstate = maxraftstate;
        let state = rf.state.clone();
        let node = raft::Node::new(rf);
        let instance = storage.clone();
        let node_for_apply = node.clone();

        // let mut term = 0;
        let stream =
            apply_ch.for_each(move | msg: ApplyMsg| {
                    let mut store = instance.lock().unwrap();
                    match msg.apply_type {
                        ApplyType::ApplyMessage => if msg.command_valid {
                            match labcodec::decode(msg.command.as_slice()) {
                                Ok(req) => {
                                    println!("begin {} apply an entry of command_index: {}", me, msg.command_index);
                                    store.apply(req, msg.command_index, msg.log_term, me);
                                },
                                Err(e) => panic!("decode error")
                            }
                        },
                        ApplyType::ApplySnapshot => match labcodec::decode(msg.command.as_slice()) {
                            Ok(req) => {
                                store.apply_snapshot(req);
                            },
                            Err(e) => panic!("decode snapshot error")
                        },
                        ApplyType::MakeSnapshot => {
                            let snap = store.make_snapshot();
                            let mut data = Vec::new();
                            labcodec::encode(&snap, &mut data).unwrap();
                            let snapshot = Snapshot {
                                last_log_term : msg.log_term,
                                last_log_index : msg.log_index,
                                last_command_index : msg.command_index,
                                data
                            };
                            node_for_apply.advance_snapshot(snapshot);
                        }
                    }
                Ok(())
            }).map_err(move |e| {
                println!("raft apply stopped: {:?}", e)
            });

        let worker = CpuPool::new(1);
        worker.spawn(stream).forget();
        let instance2 = storage.clone();
        let dur = Duration::from_millis(500);
        let stream2 = Interval::new(dur)
            .for_each(move | ()| {
                if !state.lock().unwrap().is_leader {
                    instance2.lock().unwrap().clear_callback();
                }
                // println!("{} tick raft", me);
                Ok(())
            })
            .map_err(move |e| debug!("raft tick stopped: {:?}",  e));
        worker.spawn(stream2).forget();
        KvServer {
            me,
            maxraftstate,
            worker,
            storage : storage,
            rf : node
        }
    }
}

// Choose concurrency paradigm.
//
// You can either drive the kv server by the rpc framework,
//
// ```rust
// struct Node { server: Arc<Mutex<KvServer>> }
// ```
//
// or spawn a new thread runs the kv server and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your definitions here.
    kv : Arc<KvServer>,
}

impl Node {
    pub fn new(kv: KvServer) -> Node {
        // Your code here.
        Node {
            kv : Arc::new(kv),
        }
    }

    /// the tester calls Kill() when a KVServer instance won't
    /// be needed again. you are not required to do anything
    /// in Kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        self.kv.rf.kill();
        self.kv.storage.lock().unwrap().clear_callback();
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().is_leader()
    }

    pub fn get_state(&self) -> raft::State {
        // Your code here.
        self.kv.rf.get_state()
    }
}

impl KvService for Node {


    fn get(&self, arg: GetRequest) -> RpcFuture<GetReply> {
        // Your code here.
        if !self.is_leader() {
            return Box::new(FutureResult(Ok(GetReply {
                    wrong_leader : true,
                    err : String::default(),
                    value : String::default(),
                })));
        }

        let req = PutAppendRequest {
            key : arg.key.clone(),
            op : Operator::Unknown as i32,
            value : String::from(""),
            req_id : 0
        };
        println!("{} begin get request", self.kv.rf.me);
        let (tx, rx) = oneshot();
        let (index, term) = {
            match self.kv.rf.start(&req) {
                Ok((idx, t)) => {
                    let mut store = self.kv.storage.lock().unwrap();
                    store.add_sender(idx, tx);
                    (idx, t)
                },
                Err(e) => {
                    println!("{} get request failed, try other", self.kv.rf.me);
                    match e {
                        RaftError::Rpc(error) => {
                            return Box::new(FutureResult(Err(error)));
                        },
                        RaftError::Encode(error) => {
                            return Box::new(FutureResult(Err(RpcError::Encode(error))));
                        },
                        RaftError::NotLeader => {
                            return Box::new(FutureResult(Err(RpcError::Other("not leader".to_string()))));
                        },
                        _ => panic!("error type while get ")
                    }
                }
            }
        };
        return Box::new(rx.map(move | msg : ApplyResult | {
            if term != msg.term {
                return GetReply {
                    wrong_leader : true,
                    err : String::from("term error"),
                    value : String::from(""),
                };
            } else {
                return GetReply {
                    wrong_leader :  false,
                    err : String::from(""),
                    value : msg.value,
                };
            }
        }).map_err(move |e| {
            return RpcError::Other(String::from("get rpc failed, sender cancel"));
        }));
    }

    fn put_append(&self, arg: PutAppendRequest) -> RpcFuture<PutAppendReply> {
        // Your code here.
        if !self.is_leader() {
            return Box::new(FutureResult(Ok(PutAppendReply {
                    wrong_leader : true,
                    err : String::from(""),
                })));
        }
        if self.kv.storage.lock().unwrap().operators.contains(&arg.req_id) {
            return Box::new(FutureResult(Ok(PutAppendReply {
                    wrong_leader : false,
                    err : String::from(""),
                })));
        }
        println!("{} begin put append request, op: {}, key: {}, value: {}", self.kv.rf.me, arg.op as i32, arg.key, arg.value);
        let (tx, rx) = oneshot();
        let (index, term) = {
            match self.kv.rf.start(&arg) {
                Ok((idx, t)) => {
                    let mut store = self.kv.storage.lock().unwrap();
                    store.add_sender(idx, tx);
                    (idx, t)
                },
                Err(e) => {
                    match e {
                        RaftError::Rpc(error) => {
                            return Box::new(FutureResult(Err(error)));
                        },
                        RaftError::Encode(error) => {
                            return Box::new(FutureResult(Err(RpcError::Encode(error))));
                        },
                        RaftError::NotLeader => {
                            return Box::new(FutureResult(Err(RpcError::Other("not leader".to_string()))));
                        },
                        _ => panic!("error type while get ")
                    }
                }
            }
        };
        println!("{} wait put append request, key: {}, value: {}, index: {}", self.kv.rf.me, arg.key, arg.value, index);
        let me = self.kv.rf.me;
        let (key, value) = (arg.key.clone(), arg.value.clone());
        return Box::new(rx.map(move | msg : ApplyResult | {
            if term != msg.term {
                println!("{} end put append request failed, key: {}, value: {}", me, key, value);
                return PutAppendReply {
                    wrong_leader : true,
                    err : String::from("term error"),
                };
            } else {
                println!("{} end put append request success, key: {}, value: {}", me, key, value);
                return PutAppendReply {
                    wrong_leader :  false,
                    err : String::from(""),
                };
            }
        }).map_err(move |e| {
            //println!("{} end put append request error, key: {}, value: {}", me, key, value);
            println!("{} end put append request error", me);
            return RpcError::Other(String::from("get rpc failed, sender cancel"));
        }));
    }
}
