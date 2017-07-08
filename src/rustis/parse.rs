use nom::{IResult, space, alpha, alphanumeric, digit};
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
    tag!("\"") >>
    chars: char_sequence >>
    tag!("\"") >>
    (chars)
));

named!(parsed_digit<&str, i64>, do_parse!(
    val: digit >>
    (val.parse::<i64>().unwrap())
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
    chars: alt!(
        quoted_char_sequence |
        char_sequence
    ) >>
    (Value::StrValue(chars.to_string()))
));

named!(get_parser<&str, Command>, ws!(do_parse!(
    tag_no_case!("GET") >>
    key: key_parser >>
    (Command::Get {key: key})
)));

named!(set_parser<&str, Command>, ws!(do_parse!(
    tag_no_case!("SET") >>
    key: key_parser >>
    value: strvalue_parser >>
    (Command::Set {key: key, value: value, exp: None})
)));

named!(del_parser<&str, Command>, ws!(do_parse!(
    tag_no_case!("DEL") >>
    keys: many1!(key_parser) >>
    (Command::Del {keys: keys})
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

named!(dbsize_parser<&str, Command>, do_parse!(
    tag_no_case!("DBSIZE") >>
    (Command::DbSize)
));

named!(pub command_parser<&str, Command>, alt!(
    dbsize_parser |
    get_parser |
    set_parser |
    del_parser |
    incrby_parser |
    incr_parser
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
fn test_parse_command() {
    assert_eq!(command_parser("DBSIZE"), IResult::Done("", Command::DbSize));
    assert_eq!(command_parser("GET abcd"), IResult::Done("", Command::Get {key: "abcd".to_string()}));
    assert_eq!(command_parser("DEL abcd efgh"), IResult::Done("", Command::Del {keys: vec!["abcd".to_string(), "efgh".to_string()]}));
    assert_eq!(command_parser("INCR abcd"), IResult::Done("", Command::Incr {key: "abcd".to_string()}));
    assert_eq!(command_parser("INCRBY abcd 10"), IResult::Done("", Command::IncrBy {key: "abcd".to_string(), increment: 10}));
}
