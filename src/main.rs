#![allow(dead_code)]

use std::collections::HashMap;
use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::mpsc::channel;
use std::sync::mpsc::sync_channel;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::mpsc::SyncSender;
use std::thread;
use std::time::SystemTime;

#[derive(Debug)]
enum RESPData {
    SimpleStrings(String),
    Errors(String),
    Integers(i64),
    BulkStrings(String),
    Arrays(Vec<String>),
}

enum Commands {
    Set(String, String, Option<u128>),
    Get(String, SyncSender<String>),
}

fn encode(msg: &str) -> Vec<u8> {
    if msg.starts_with("$") {
        msg.as_bytes().to_vec()
    } else {
        Vec::from(format!("+{}\r\n", msg).as_bytes())
    }
}

// Returns the number of milliseconds since Unix epoch.
fn time_now() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis()
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

fn manage_store(mut store: HashMap<String, (String, Option<u128>)>, rx: Receiver<Commands>) {
    for received in rx {
        match received {
            Commands::Set(k, v, None) => {
                store.insert(k, (v, None));
            }
            Commands::Set(k, v, Some(px)) => {
                let expiry = px + time_now();
                println!("expiry {}", expiry);
                store.insert(k, (v, Some(expiry)));
            }
            Commands::Get(k, tx) => {
                let v = store.get(&k).unwrap().to_owned();
                match v.1 {
                    Some(x) => {
                        if time_now() >= x {
                            tx.send("$-1".to_string()).unwrap();
                        } else {
                            tx.send(v.0).unwrap();
                        }
                    }
                    None => {
                        tx.send(v.0).unwrap();
                    }
                }
            }
        };
    }
}

fn handle_stream(mut stream: TcpStream, tx: Sender<Commands>) {
    println!("connected {}", stream.peer_addr().unwrap());
    loop {
        let mut buffer = [0_u8; 1024];
        match stream.read(&mut buffer) {
            Ok(n) if n > 0 => {
                let respd = decode(&buffer[..n]);
                println!("respd: {:?}", respd);
                match respd {
                    RESPData::Arrays(v) if v.first().unwrap().to_uppercase() == "ECHO" => {
                        println!("to reply: {}", &v[2]);
                        stream.write(&encode(&v[2])).unwrap();
                    }
                    RESPData::Arrays(v) if v.first().unwrap().to_uppercase() == "SET" => {
                        let k = &v[2];
                        let px = v
                            .iter()
                            .enumerate()
                            .find(|(_, x)| x.to_ascii_uppercase() == "PX");
                        let v = match px {
                            Some((idx, _)) => {
                                (String::from(&v[4]), str::parse::<u128>(&v[idx + 2]).ok())
                            }
                            None => (String::from(&v[4]), None),
                        };
                        tx.send(Commands::Set(k.to_owned(), v.0, v.1)).unwrap();
                        stream.write(&encode("OK")).unwrap();
                    }
                    RESPData::Arrays(v) if v.first().unwrap().to_uppercase() == "GET" => {
                        let k = &v[2];
                        let (mtx, mrx) = sync_channel(0);
                        tx.send(Commands::Get(k.to_owned(), mtx)).unwrap();
                        let v = mrx.recv().unwrap();
                        stream.write(&encode(&v)).unwrap();
                    }
                    RESPData::SimpleStrings(_) | RESPData::Arrays(_) => {
                        stream.write(&encode("PONG")).unwrap();
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

    let (tx, rx) = channel();
    let hm = HashMap::new();
    thread::spawn(move || manage_store(hm, rx));

    for stream in listener.incoming() {
        let tx = tx.clone();
        match stream {
            Ok(stream) => {
                thread::spawn(move || handle_stream(stream, tx));
            }
            Err(e) => println!("couldn't get client: {e:?}"),
        }
    }
}
