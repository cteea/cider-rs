#![allow(dead_code)]

use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::net::TcpStream;
use std::thread;

#[derive(Debug)]
enum RESPData {
    SimpleStrings(String),
    Errors(String),
    Integers(i64),
    BulkStrings(String),
    Arrays(Vec<String>),
}

fn encode(msg: &str) -> Vec<u8> {
    Vec::from(format!("+{}\r\n", msg).as_bytes())
}

fn decode(data: &[u8]) -> RESPData {
    let tokens: Vec<String> = String::from_utf8_lossy(data)
        .trim()
        .split("\r\n")
        .map(|x| x.to_owned())
        .collect();
    let dt = tokens.first().unwrap().get(0..1).unwrap();
    match dt {
        "+" => RESPData::SimpleStrings(tokens[1].to_string()),
        "*" => RESPData::Arrays(tokens[2..].iter().map(|e| e.to_string()).collect()),
        _ => unimplemented!(),
    }
}

fn handle_stream(mut stream: TcpStream) {
    println!("connected {}", stream.peer_addr().unwrap());
    loop {
        let mut buffer = [0_u8; 1024];
        match stream.read(&mut buffer) {
            Ok(n) if n > 0 => {
                let respd = decode(&buffer[..n]);
                match respd {
                    RESPData::Arrays(v) if v.first().unwrap().to_uppercase() == "ECHO" => {
                        stream.write(&encode(&v[2])).unwrap()
                    }
                    RESPData::SimpleStrings(_) | RESPData::Arrays(_) => {
                        stream.write(&encode("PONG")).unwrap()
                    }
                    _ => unimplemented!(),
                };
            }
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => break,
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => break,
            Err(e) => panic!("encountered IO error: {}", e),
        }
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(move || handle_stream(stream));
            }
            Err(e) => println!("couldn't get client: {e:?}"),
        }
    }
}
