use nom::{IResult, digit};
use rustis::key::Key;
use rustis::command::Command;
use rustis::value::Value;

// represents the number of characters consumed, plus a Vec of parsed commands
#[derive(Debug, PartialEq)]
pub struct ParseResult(pub usize, pub Vec<Command>);

// parse the RESP protocol as a Vec<&str>

named!(str_prefix<&str, usize>, do_parse!(
    tag!("$") >>
    length: digit >>
    tag!("\r\n") >>
    (length.parse::<usize>().unwrap())
));

named!(str_parser<&str, &str>, do_parse!(
    length: str_prefix >>
    chars: take!(length) >>
    tag!("\r\n") >>
    (chars)
));

named!(array_prefix<&str, usize>, do_parse!(
    tag!("*") >>
    length: digit >>
    tag!("\r\n") >>
    (length.parse::<usize>().unwrap())
));

named!(pub resp_array_parser<&str, Vec<&str>>, do_parse!(
    length: array_prefix >>
    strings: count!(str_parser, length) >>
    (strings)
));

// parse the space-joined Vec<&str> into a command

named!(char_sequence<&str, &str>, do_parse!(
    chars: is_not!("\" \r\n") >>
    (chars)
));

named!(quoted_char_sequence<&str, &str>, do_parse!(
    char!('"') >>
    chars: opt!(is_not!("\"")) >>
    char!('"') >>
    (match chars {
        Some(x) => x,
        None => ""
    })
));

named!(parsed_udigit<&str, i64>, do_parse!(
    val: digit >>
    (val.parse::<i64>().unwrap())
));

named!(parsed_digit<&str, i64>, do_parse!(
    sign: opt!(complete!(tag!("-"))) >>
    val: parsed_udigit >>
    ((match sign {
        Some("-") => -1,
        _ => 1,
    }) * val)
));

named!(parsed_float<&str, f64>, do_parse!(
    base: parsed_digit >>
    dec: opt!(complete!(do_parse!(tag!(".") >> n: digit >> (n)))) >>
    (format!("{}.{}", base, match dec {
        Some(x) => x,
        None => "0",
    }).parse::<f64>().unwrap())
));

named!(parsed_string<&str, &str>, do_parse!(
    chars: alt!(
        quoted_char_sequence |
        char_sequence
    ) >>
    (chars)
));

named!(key_parser<&str, Key>, do_parse!(
    char: char_sequence >>
    (char.to_string())
));

named!(intvalue_parser<&str, Value>, do_parse!(
    val: parsed_digit >>
    (Value::IntValue(val))
));

named!(strvalue_parser<&str, Value>, do_parse!(
    chars: parsed_string >>
    (Value::StrValue(chars.to_string()))
));

named!(value_parser<&str, Value>, do_parse!(
    val: alt!(intvalue_parser | strvalue_parser) >>
    (val)
));

named!(get_parser<&str, Command>, ws!(do_parse!(
    tag_no_case!("GET") >>
    key: key_parser >>
    (Command::Get {key: key})
)));

named!(set_parser<&str, Command>, ws!(do_parse!(
    tag_no_case!("SET") >>
    key: key_parser >>
    value: value_parser >>
    (Command::Set {key: key, value: value, exp: None})
)));

named!(append_parser<&str, Command>, ws!(do_parse!(
    tag_no_case!("APPEND") >>
    key: key_parser >>
    value: parsed_string >>
    (Command::Append {key: key, value: value.to_string()})
)));

named!(del_parser<&str, Command>, ws!(do_parse!(
    tag_no_case!("DEL") >>
    keys: many1!(key_parser) >>
    (Command::Del {keys: keys})
)));

named!(exists_parser<&str, Command>, ws!(do_parse!(
    tag_no_case!("EXISTS") >>
    key: key_parser >>
    (Command::Exists {key: key})
)));

named!(type_parser<&str, Command>, ws!(do_parse!(
    tag_no_case!("TYPE") >>
    key: key_parser >>
    (Command::Type {key: key})
)));

named!(incr_parser<&str, Command>, ws!(do_parse!(
    tag_no_case!("INCR") >>
    key: key_parser >>
    (Command::Incr {key: key})
)));

named!(incrby_parser<&str, Command>, ws!(do_parse!(
    tag_no_case!("INCRBY") >>
    key: key_parser >>
    increment: parsed_digit >>
    (Command::IncrBy {key: key, increment: increment})
)));

named!(incrbyfloat_parser<&str, Command>, ws!(do_parse!(
    tag_no_case!("INCRBYFLOAT") >>
    key: key_parser >>
    increment: parsed_float >>
    (Command::IncrByFloat {key: key, increment: increment})
)));

named!(decr_parser<&str, Command>, ws!(do_parse!(
    tag_no_case!("DECR") >>
    key: key_parser >>
    (Command::Decr {key: key})
)));

named!(decrby_parser<&str, Command>, ws!(do_parse!(
    tag_no_case!("DECRBY") >>
    key: key_parser >>
    decrement: parsed_digit >>
    (Command::DecrBy {key: key, decrement: decrement})
)));

named!(select_parser<&str, Command>, ws!(do_parse!(
    tag_no_case!("SELECT") >>
    db: parsed_digit >>
    (Command::Select(db as usize))
)));

named!(flushdb_parser<&str, Command>, ws!(do_parse!(
    tag_no_case!("FLUSHDB") >>
    (Command::FlushDb)
)));

named!(flushall_parser<&str, Command>, ws!(do_parse!(
    tag_no_case!("FLUSHALL") >>
    (Command::FlushAll)
)));

named!(swapdb_parser<&str, Command>, ws!(do_parse!(
    tag_no_case!("SWAPDB") >>
    db1: parsed_digit >>
    db2: parsed_digit >>
    (Command::SwapDb(db1 as usize, db2 as usize))
)));

named!(dbsize_parser<&str, Command>, ws!(do_parse!(
    tag_no_case!("DBSIZE") >>
    (Command::DbSize)
)));

named!(ping_parser<&str, Command>, ws!(do_parse!(
    tag_no_case!("PING") >>
    message: opt!(complete!(parsed_string)) >>
    (Command::Ping {message: match message {
        Some(x) => x.to_string(),
        None => "PONG".to_string(),
    }})
)));

named!(echo_parser<&str, Command>, ws!(do_parse!(
    tag_no_case!("ECHO") >>
    message: parsed_string >>
    (Command::Echo {message: message.to_string()})
)));

named!(pub command_parser<&str, Command>, alt!(
    select_parser |
    flushdb_parser |
    flushall_parser |
    swapdb_parser |
    dbsize_parser |
    get_parser |
    set_parser |
    append_parser |
    del_parser |
    exists_parser |
    type_parser |
    incrbyfloat_parser |
    incrby_parser |
    incr_parser |
    decrby_parser |
    decr_parser |
    echo_parser |
    ping_parser
));


#[test]
fn test_parse_resp() {
    assert_eq!(str_prefix("$0\r\n"), IResult::Done("", 0));
    assert_eq!(str_prefix("$1\r\n"), IResult::Done("", 1));
    assert_eq!(str_prefix("$80\r\n"), IResult::Done("", 80));

    assert_eq!(str_parser("$0\r\n\r\n"), IResult::Done("", ""));
    assert_eq!(str_parser("$1\r\nx\r\n"), IResult::Done("", "x"));
    assert_eq!(str_parser("$4\r\nabcd\r\n"), IResult::Done("", "abcd"));

    assert_eq!(array_prefix("*0\r\n"), IResult::Done("", 0));
    assert_eq!(array_prefix("*1\r\n"), IResult::Done("", 1));
    assert_eq!(array_prefix("*80\r\n"), IResult::Done("", 80));

    assert_eq!(resp_array_parser("*0\r\n"), IResult::Done("", vec![]));
    assert_eq!(resp_array_parser("*1\r\n$12\r\nabcdefghijkl\r\n"), IResult::Done("", vec!["abcdefghijkl"]));
    assert_eq!(resp_array_parser("*2\r\n$0\r\n\r\n$4\r\nabcd\r\n"), IResult::Done("", vec!["", "abcd"]));
    assert_eq!(resp_array_parser("*1\r\n$6\r\nDBSIZE\r\n"), IResult::Done("", vec!["DBSIZE"]));
}

#[test]
fn test_parse_int() {
    assert_eq!(parsed_digit("123"), IResult::Done("", 123));
    assert_eq!(parsed_digit("0"), IResult::Done("", 0));
    assert_eq!(parsed_digit("-123"), IResult::Done("", -123));
}

#[test]
fn test_parse_float() {
    assert_eq!(parsed_float("1"), IResult::Done("", 1.0));
    assert_eq!(parsed_float("1.0"), IResult::Done("", 1.0));
    assert_eq!(parsed_float("1.2"), IResult::Done("", 1.2));
}

#[test]
fn test_parse_quoted_chars() {
    assert_eq!(quoted_char_sequence("\"\""), IResult::Done("", ""));
    assert_eq!(quoted_char_sequence("\"abc123def\""), IResult::Done("", "abc123def"));
    assert_eq!(quoted_char_sequence("\" \""), IResult::Done("", " "));
    assert_eq!(quoted_char_sequence("\"hello world\""), IResult::Done("", "hello world"));
}

#[test]
fn test_parse_command() {
    assert_eq!(command_parser("SELECT 1"), IResult::Done("", Command::Select(1)));
    assert_eq!(command_parser("DBSIZE"), IResult::Done("", Command::DbSize));
    assert_eq!(command_parser("FLUSHALL"), IResult::Done("", Command::FlushAll));
    assert_eq!(command_parser("FLUSHDB"), IResult::Done("", Command::FlushDb));
    assert_eq!(command_parser("GET abcd"), IResult::Done("", Command::Get {key: "abcd".to_string()}));
    assert_eq!(command_parser("SET abc 1"), IResult::Done("", Command::Set {key: "abc".to_string(), value: Value::IntValue(1), exp: None}));
    assert_eq!(command_parser("EXISTS abcd"), IResult::Done("", Command::Exists {key: "abcd".to_string()}));
    assert_eq!(command_parser("TYPE abcd"), IResult::Done("", Command::Type {key: "abcd".to_string()}));
    assert_eq!(command_parser("DEL abcd efgh"), IResult::Done("", Command::Del {keys: vec!["abcd".to_string(), "efgh".to_string()]}));
    assert_eq!(command_parser("INCR abcd"), IResult::Done("", Command::Incr {key: "abcd".to_string()}));
    assert_eq!(command_parser("INCRBY abcd 10"), IResult::Done("", Command::IncrBy {key: "abcd".to_string(), increment: 10}));
    assert_eq!(command_parser("INCRBYFLOAT abcd 0.1"), IResult::Done("", Command::IncrByFloat {key: "abcd".to_string(), increment: 0.1}));
    assert_eq!(command_parser("PING"), IResult::Done("", Command::Ping {message: "PONG".to_string()}));
    assert_eq!(command_parser("ECHO \"hello world\""), IResult::Done("", Command::Echo {message: "hello world".to_string()}));
}
