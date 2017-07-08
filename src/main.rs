pub mod rustis;

#[macro_use]
extern crate nom;
extern crate argparse;
extern crate mio;

use argparse::{ArgumentParser, Store};
use rustis::server::RustisServer;

fn main() {
    // parse CLI args
    let mut src = "localhost:6379".to_string();
    let mut db_count = 16;
    {
        let mut parser = ArgumentParser::new();
        parser.refer(&mut src).add_argument("address", Store, "host:port to listen on");
        parser.refer(&mut db_count).add_option(&["-d", "--db-count"], Store, "number of separate redis DBs to run");

        parser.parse_args_or_exit();
    }

    let mut server = RustisServer::new(db_count);
    server.run(src);
}
