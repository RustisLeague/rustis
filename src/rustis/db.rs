use std::collections::HashMap;
use std::collections::binary_heap::BinaryHeap;
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

    pub fn parse_command(&mut self, c:&str) {

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
