use std::fmt::{Display, Formatter, Result};

#[derive(Clone, PartialEq, Debug)]
pub enum Value {
    Nil,
    IntValue(i64),
    StrValue(String),
    ArrayValue(Vec<Value>),
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter) -> Result {
        match self {
            &Value::Nil => write!(f, "$-1\r\n"),
            &Value::IntValue(ref i) => write!(f, ":{}\r\n", i),
            &Value::StrValue(ref s) => write!(f, "${}\r\n{}\r\n", s.len(), s),
            &Value::ArrayValue(ref a) => write!(f, "*{}\r\n{}", a.len(), a.iter().map(|ref x| format!("{}", x)).collect::<Vec<String>>().join("")),
        }
    }
}

#[test]
fn test_nil() {
    assert_eq!(format!("{}", Value::Nil), "$-1\r\n");
}

#[test]
fn test_int() {
    assert_eq!(format!("{}", Value::IntValue(1)), ":1\r\n");
    assert_eq!(format!("{}", Value::IntValue(20)), ":20\r\n");
    assert_eq!(format!("{}", Value::IntValue(500)), ":500\r\n");
    assert_eq!(format!("{}", Value::IntValue(-12)), ":-12\r\n");
}

#[test]
fn test_str() {
    assert_eq!(format!("{}", Value::StrValue("".to_string())), "$0\r\n\r\n");
    assert_eq!(format!("{}", Value::StrValue("a".to_string())), "$1\r\na\r\n");
    assert_eq!(format!("{}", Value::StrValue("abc def".to_string())), "$7\r\nabc def\r\n");
    assert_eq!(format!("{}", Value::StrValue("abc\ndefg".to_string())), "$8\r\nabc\ndefg\r\n");
}

#[test]
fn test_array() {
    assert_eq!(format!("{}", Value::ArrayValue(vec![Value::IntValue(1), Value::Nil, Value::StrValue("abc".to_string())])), "*3\r\n:1\r\n$-1\r\n$3\r\nabc\r\n");
}
