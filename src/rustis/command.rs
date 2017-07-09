use std::fmt::{Display, Formatter, Result};
use std::option::Option;
use nom::{IResult};
use rustis::key::Key;
use rustis::value::Value;
use rustis::parse::{ParseResult, resp_array_parser, command_parser};

#[derive(Debug, PartialEq)]
pub enum Command {
    // strings
    Set {key:Key, value:Value, exp:Option<u64>},
    Append {key:Key, value:String},
    Get {key:Key},
    Incr {key:Key},
    IncrBy {key:Key, increment:i64},
    Decr {key:Key},
    DecrBy {key:Key, decrement:i64},
    // all
    Del {keys:Vec<Key>},
    Exists {key:Key},
    // misc
    DbSize,
    Select(usize),
    FlushDb,
    FlushAll,
    SwapDb(usize, usize),
    Ping {message:String},
    Echo {message:String},
}

#[derive(Debug, PartialEq)]
pub enum Return {
    Ok,
    Error(String),
    ValueReturn(Value),
}

impl Command {
    pub fn quote(s:&str) -> String {
        return if s.len() == 0 || s.contains(" ") {
            format!("\"{}\"", s)
        } else {
            s.to_string()
        };
    }

    pub fn parse(s:&str) -> ParseResult {
        let mut remaining = s;
        let mut parsed_chars = 0;
        let mut commands = Vec::new();
        while s.len() > 0 {
            let parts = resp_array_parser(remaining);
            match parts {
                IResult::Done(r, x) => {
                    parsed_chars += remaining.len() - r.len();
                    remaining = r;
                    let joined = x.iter().map(|i| Command::quote(i)).collect::<Vec<String>>().join(" ");
                    let cmd = command_parser(&joined);
                    match cmd {
                        IResult::Done("", c) => {
                            commands.push(c);
                        }
                        _ => {
                            break;
                        }
                    }
                }
                _ => {
                    break;
                }
            }
        }
        return ParseResult(parsed_chars, commands);
    }
}

impl Display for Return {
    fn fmt(&self, f: &mut Formatter) -> Result {
        match self {
            &Return::Ok => write!(f, "+OK\r\n"),
            &Return::Error(ref s) => write!(f, "-{}\r\n", s),
            &Return::ValueReturn(ref v) => {
                return v.fmt(f);
            }
        }
    }
}

#[test]
fn test_ok() {
    assert_eq!(format!("{}", Return::Ok), "+OK\r\n");
}

#[test]
fn test_error() {
    assert_eq!(format!("{}", Return::Error("ERR there was an error".to_string())), "-ERR there was an error\r\n");
}

#[test]
fn test_parse() {
    assert_eq!(
        Command::parse("*1\r\n$6\r\nDBSIZE\r\n"),
        ParseResult(16, vec![Command::DbSize])
    );
    assert_eq!(
        Command::parse("*1\r\n$6\r\nDBSIZE\r\n*2\r\n$3\r\nGET\r\n$4\r\nabcd\r\n"),
        ParseResult(39, vec![Command::DbSize, Command::Get {key: "abcd".to_string()}])
    );
}
