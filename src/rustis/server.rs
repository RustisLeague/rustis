use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::SocketAddr;
use mio::*;
use mio::unix::*;
use mio::tcp::{TcpListener, TcpStream};
use rustis::command::{Command, Return};
use rustis::db::RustisDb;
use rustis::parse::ParseResult;

const LISTENER:Token = Token(0);
const MAX_CONNECTIONS:usize = 0x1000;
const EVENT_PREALLOCATE:usize = 0x400;


struct ClientConnection {
    stream: TcpStream,
    buf: String,
    db: usize,
}

impl ClientConnection {
    pub fn new(stream:TcpStream) -> ClientConnection {
        ClientConnection {
            stream: stream,
            buf: String::new(),
            db: 0,
        }
    }
}

pub struct RustisServer {
    client_tokens:Vec<usize>,
    poll:Poll,
    connections:HashMap<usize, ClientConnection>,
    dbs:Vec<RustisDb>,
}

impl RustisServer {
    pub fn new(db_count:usize) -> RustisServer {
        let mut dbs = Vec::with_capacity(db_count);
        for _ in 0..db_count {
            dbs.push(RustisDb::new());
        }
        RustisServer {
            client_tokens: (1..MAX_CONNECTIONS+1).collect::<Vec<usize>>(),
            poll: Poll::new().unwrap(),
            connections: HashMap::new(),
            dbs: dbs,
        }
    }

    pub fn run(&mut self, src:String) {
        println!("rustis server listening on {}...", src);
        let addr = src.parse::<SocketAddr>().unwrap();
        let server = TcpListener::bind(&addr).unwrap();
        self.poll.register(&server, LISTENER, Ready::readable(), PollOpt::edge()).unwrap();
        let mut events = Events::with_capacity(EVENT_PREALLOCATE);

        loop {
            self.poll.poll(&mut events, None).unwrap();

            for event in events.iter() {
                match event.token() {
                    LISTENER => {
                        let (s, _) = server.accept().unwrap();
                        let token = self.get_client_token();
                        self.poll.register(&s, Token(token), Ready::readable() | UnixReady::hup(), PollOpt::edge()).unwrap();
                        self.connections.insert(token, ClientConnection::new(s));
                        println!("new connection");
                    }
                    Token(t) => {
                        let read = event.readiness().contains(Ready::readable());
                        let mut hup = event.readiness().contains(UnixReady::hup());
                        if read {
                            let mut connection = self.connections.get_mut(&t).unwrap();
                            let stream = &mut connection.stream;
                            let buf = &mut connection.buf;
                            stream.read_to_string(buf);
                            let parse = Command::parse(buf);
                            match parse {
                                ParseResult(parsed_chars, mut c) => {
                                    while c.len() > 0 {
                                        let cmd = c.remove(0);
                                        let mut should_run = true;
                                        match cmd {
                                            Command::Select(db) => {
                                                if db < self.dbs.len() {
                                                    connection.db = db;
                                                } else {
                                                    should_run = false;
                                                    stream.write_fmt(format_args!("{}", Return::Error("ERR db out of range".to_string()))).unwrap();
                                                }
                                            }
                                            Command::SwapDb(db1, db2) => {
                                                let dbs = &mut self.dbs;
                                                dbs.swap(db1, db2);
                                            }
                                            Command::FlushAll => {
                                                let dbs = &mut self.dbs;
                                                for db in dbs {
                                                    db.run_command(Command::FlushDb);
                                                }
                                            }
                                            _ => {}
                                        }
                                        if should_run {
                                            let result = self.dbs[connection.db].run_command(cmd);
                                            stream.write_fmt(format_args!("{}", result)).unwrap();
                                        }
                                    }
                                    buf.drain(0..parsed_chars);
                                }
                            }
                        }
                        if hup {
                            let connection = self.connections.remove(&t).unwrap();
                            self.poll.deregister(&connection.stream).unwrap();
                            self.recycle_client_token(t);
                            println!("hup");
                        }
                    }
                }
            }
        }
    }

    fn get_client_token(&mut self) -> usize {
        return self.client_tokens.pop().unwrap();
    }

    fn recycle_client_token(&mut self, token:usize) {
        self.client_tokens.push(token);
    }
}
