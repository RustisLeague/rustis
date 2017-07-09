use std::collections::{BinaryHeap, HashMap};
use rustis::command::{Command, Return};
use rustis::key::{ExpireTime, Key};
use rustis::value::Value;

pub struct RustisDb {
    values:HashMap<Key, Value>,
    exp:BinaryHeap<ExpireTime>,
}

impl RustisDb {
    pub fn new() -> RustisDb {
        return RustisDb {
            values: HashMap::with_capacity(1024),
            exp: BinaryHeap::with_capacity(1024),
        };
    }

    pub fn gc(&mut self) {
        // TODO: remove any keys that have expired
    }

    pub fn run_command(&mut self, cmd:Command) -> Return {
        match cmd {
            Command::Get {key} => {
                let value:Value = match self.values.get_mut(&key) {
                    Some(v) => v.clone(),
                    None => Value::Nil,
                };
                return Return::ValueReturn(value);
            }
            Command::Set {key, value, exp} => {
                // TODO: remove expiration if it exists
                self.values.insert(key, value);
                match exp {
                    Some(x) => {
                        // TODO: add expiration time
                    }
                    None => {}
                }
                return Return::Ok;
            }
            Command::Append {key, value} => {
                let new_value = match self.values.get(&key) {
                    Some(x) => {
                        match x {
                            &Value::StrValue(ref s) => format!("{}{}", s, value),
                            &Value::IntValue(ref i) => format!("{}{}", i, value),
                            _ => return Return::Error("WRONGTYPE key doesn't contain a string".to_string()),
                        }
                    }
                    None => {
                        value
                    }
                };
                let return_value = Return::ValueReturn(Value::IntValue(new_value.len() as i64));
                self.values.insert(key, Value::StrValue(new_value));
                return return_value;
            }
            Command::Incr {key} => {
                return self.run_command(Command::IncrBy {key: key, increment: 1});
            }
            Command::Decr {key} => {
                return self.run_command(Command::IncrBy {key: key, increment: -1});
            }
            Command::DecrBy {key, decrement} => {
                return self.run_command(Command::IncrBy {key: key, increment: -decrement});
            }
            Command::IncrBy {key, increment} => {
                let new_value = match self.values.get(&key) {
                    Some(&Value::IntValue(ref i)) =>
                    {
                        Value::IntValue(i + increment)
                    }
                    Some(&Value::StrValue(ref s)) => {
                        let parsed = s.parse::<i64>();
                        match parsed {
                            Ok(i) => Value::IntValue(i + increment),
                            Err(_) => return Return::Error("ERR value is not an integer or out of range".to_string()),
                        }
                    }
                    _ => {
                        Value::IntValue(increment)
                    }
                };
                let return_value = new_value.clone();
                self.values.insert(key, new_value);
                return Return::ValueReturn(return_value);
            }
            Command::DbSize => {
                return Return::ValueReturn(Value::IntValue(self.values.len() as i64));
            }
            Command::Del {keys} => {
                // TODO: remove expiration if it exists
                let mut i = 0;
                for key in keys.iter() {
                    match self.values.remove(key) {
                        Some(_) => {
                            i += 1;
                        }
                        None => {}
                    }
                }
                return Return::ValueReturn(Value::IntValue(i));
            }
            Command::Ping {message} => {
                return self.run_command(Command::Echo {message: message});
            }
            Command::Echo {message} => {
                return Return::ValueReturn(Value::StrValue(message));
            }
            Command::Exists {key} => {
                return Return::ValueReturn(Value::IntValue(if self.values.contains_key(&key) {1} else {0}));
            }
            Command::FlushDb => {
                self.values.clear();
                return Return::Ok;
            }
            _ => {
                return Return::Ok;
            }
        }
    }
}

#[test]
fn test_dbsize() {
    let mut db = RustisDb::new();
    assert_eq!(db.run_command(Command::DbSize), Return::ValueReturn(Value::IntValue(0)));
    let result = db.run_command(Command::Set {key: "test_key123".to_string(), value: Value::StrValue("abc".to_string()), exp: None});
    assert_eq!(result, Return::Ok);
    assert_eq!(db.run_command(Command::DbSize), Return::ValueReturn(Value::IntValue(1)));
    let result = db.run_command(Command::Set {key: "test_key123".to_string(), value: Value::StrValue("def".to_string()), exp: None});
    assert_eq!(result, Return::Ok);
    assert_eq!(db.run_command(Command::DbSize), Return::ValueReturn(Value::IntValue(1)));
    let result = db.run_command(Command::Set {key: "test_key456".to_string(), value: Value::StrValue("abc".to_string()), exp: None});
    assert_eq!(result, Return::Ok);
    assert_eq!(db.run_command(Command::DbSize), Return::ValueReturn(Value::IntValue(2)));
    let result = db.run_command(Command::Del {keys: vec!["test_key456".to_string()]});
    assert_eq!(result, Return::ValueReturn(Value::IntValue(1)));
    assert_eq!(db.run_command(Command::DbSize), Return::ValueReturn(Value::IntValue(1)));
}

#[test]
fn test_get_set() {
    let mut db = RustisDb::new();
    assert_eq!(db.run_command(Command::Get {key: "test_key123".to_string()}), Return::ValueReturn(Value::Nil));
    let result = db.run_command(Command::Set {key: "test_key123".to_string(), value: Value::StrValue("def".to_string()), exp: None});
    assert_eq!(result, Return::Ok);
    assert_eq!(db.run_command(Command::Get {key: "test_key123".to_string()}), Return::ValueReturn(Value::StrValue("def".to_string())));
}

#[test]
fn test_incr() {
    let mut db = RustisDb::new();
    assert_eq!(db.run_command(Command::Incr {key: "abc".to_string()}), Return::ValueReturn(Value::IntValue(1)));
    assert_eq!(db.run_command(Command::Incr {key: "abc".to_string()}), Return::ValueReturn(Value::IntValue(2)));
    assert_eq!(db.run_command(Command::IncrBy {key: "abc".to_string(), increment: 10}), Return::ValueReturn(Value::IntValue(12)));
    db.run_command(Command::Set {key: "abc".to_string(), value: Value::StrValue("defg".to_string()), exp: None});
    assert!(match db.run_command(Command::IncrBy {key: "abc".to_string(), increment: 10}) {
        Return::Error(_) => true,
        _ => false,
    });
}
