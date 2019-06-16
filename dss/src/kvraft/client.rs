use std::fmt;
use std::sync::{Arc, Mutex};
use std::cell::RefCell;
// use labrpc::Error as RpcError;
use futures::future::Future;

use super::service;
use super::service::{PutAppendRequest, GetRequest};
use std::time::Duration;
use rand::random;

enum Op {
    Put(String, String),
    Append(String, String),
}

pub struct Clerk {
    pub name: String,
    servers: Vec<service::KvClient>,
    leader : Arc<Mutex<usize>>,
    // You will have to modify this struct.
}

impl fmt::Debug for Clerk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clerk").field("name", &self.name).finish()
    }
}

impl Clerk {
    pub fn new(name: String, servers: Vec<service::KvClient>) -> Clerk {
        // You'll have to add code here.
        Clerk { name, servers, leader : Arc::default() }
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].get(args).unwrap();
    pub fn get(&self, key: String) -> String {
        // You will have to modify this function.
        let args = GetRequest {
            key : key.clone(),
        };
        let mut leader = self.get_leader();
        let mut lp_times = 0;
        let rpc_id : u32 = random();
        println!("{} begin {} get key: {}", self.name, rpc_id, key);
        loop {
            let ret = self.servers[leader].get(&args).wait();
            match ret {
                Ok(reply) => {
                    if reply.wrong_leader {
                        leader = (leader + 1) % self.servers.len();
                    } else if !reply.err.is_empty() {
                        continue;
                    } else {
                        *self.leader.lock().unwrap() = leader;
                        println!("{} end {} get key: {}", self.name, rpc_id, key);
                        return reply.value;
                    }
                },
                Err(e) => {
                    leader = (leader + 1) % self.servers.len();
                }
            }
            lp_times += 1;
            if lp_times % self.servers.len() == 0 {
                std::thread::sleep(Duration::from_millis(1000));
            }
        }
    }

    /// shared by Put and Append.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].put_append(args).unwrap();
    fn put_append(&self, op: Op) {
        // You will have to modify this function.
        let (operator, key, value) = match op {
            Op::Append(k, v) => (service::Operator::Append, k, v),
            Op::Put(k, v) => (service::Operator::Put, k, v)
        };
        let mut req_id = 0;
        thread_local!(static FOO: RefCell<u64> = RefCell::new(random()));
        FOO.with(|f| {
            req_id = *f.borrow();
            *f.borrow_mut() = req_id + 1;
        });
        let args = PutAppendRequest {
            op : operator as i32,
            key: key.clone(),
            value: value.clone(),
            req_id
        };
        let mut leader = self.get_leader();
        let mut lp_times = 0;
        println!("{:?} begin {} put key: {}, value: {}", self.name, req_id, key, value);
        loop {
            let ret = self.servers[leader].put_append(&args).wait();
            match ret {
                Ok(reply) => {
                    if reply.wrong_leader {
                        leader = (leader + 1) % self.servers.len();
                    } else if !reply.err.is_empty() {
                        continue;
                    }
                    else {
                        break;
                    }
                },
                Err(e) => {
                    leader = (leader + 1) % self.servers.len();
                }
            }
            lp_times += 1;
            if lp_times % self.servers.len() == 0 {
                std::thread::sleep(Duration::from_millis(1000));
            }
        }
        println!("{} end {} put key: {}", self.name, req_id, key);
        *self.leader.lock().unwrap() = leader;
    }

    pub fn put(&self, key: String, value: String) {
        self.put_append(Op::Put(key, value))
    }

    pub fn append(&self, key: String, value: String) {
        self.put_append(Op::Append(key, value))
    }

    fn get_leader(&self) -> usize {
        return self.leader.lock().unwrap().clone();
    }
}
