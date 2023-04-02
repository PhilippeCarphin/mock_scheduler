use std::error::Error;
use std::io::prelude::*;
use std::io::BufReader;
use std::net::{TcpListener, TcpStream};
use std::result::Result;
use std::collections::HashMap;
use serde_json;
use serde::{Deserialize, Serialize};
use std::sync::Mutex;

static QUEUE: Mutex<Vec<Job>> = Mutex::new(Vec::<Job>::new());
static JOBID_COUNTER: Mutex<u32> = Mutex::new(0);

#[allow(dead_code)]
#[derive(Debug,Deserialize)]
struct JobSpec {
    script: String,
    #[serde(default = "Vec::<String>::new")]
    job_args: Vec<String>,
    #[serde(default = "HashMap::<String,String>::new")]
    submit_args: HashMap<String, String>,
}

#[allow(dead_code)]
#[derive(Debug)]
struct Job {
    job_spec: JobSpec,
    job_id: u32,
    running: bool,
    process: Option<u32>,
}

#[derive(Debug, Serialize)]
struct SubmitResponse {
    job_id: i32,
}

#[derive(Debug)]
enum HttpMethod {
    Post,
    Get,
    Put,
    Delete,
}

#[allow(dead_code)]
#[derive(Debug)]
struct HttpRequest {
    method:  HttpMethod,
    headers: Vec<String>, // TODO: Change to HashMap
    path:    String,
    query:   HashMap<String,String>,
    body:    String, // Could be bytes but I'm only going to be doing strings
}
#[allow(dead_code)]
#[derive(Debug)]
struct HttpResponse {
    code: u32,
    headers: HashMap<String,String>,
    body: Vec<u8>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:7878").unwrap();

    for stream in listener.incoming() {
        if let Err(e) = handle_connection(stream?) {
            println!("Error handling connection: {:?}", e);
        } else {
            println!("Successfully handled connection!");
        }
    }
    Ok(())
}

fn read_header(buf_reader: &mut BufReader<&mut TcpStream>) -> Result<Vec<String>, Box<dyn Error>> {
    let mut header: Vec<String> = vec![];
    loop {
        let mut s = String::new();
        buf_reader.read_line(&mut s)?;
        if s.is_empty() {
            println!("Header read: break because of empty");
            break;
        }
        if s == "\r\n" {
            println!("Header read: break because small line '{}'", s);
            break;
        }
        header.push(s.trim().to_string());
    }
    Ok(header)
}

fn get_content_length(header: &Vec<String>) -> Result<usize, Box<dyn Error>> {
    for l in header {
        if l.starts_with("Content-Length") {
            let parts: Vec<_> = l.split(":").collect();
            let size_str = parts.get(1).ok_or("Bad Content-Length header")?;
            let size = size_str.trim().parse::<usize>()?;
            return Ok(size);
        }
    }
    Err("No Content-Length in header".into())
}

fn get_body(
    buf_reader: &mut BufReader<&mut TcpStream>,
    size: usize,
) -> Result<String, Box<dyn Error>> {
    let mut body_bytes: Vec<u8> = vec![0; size];
    buf_reader.read_exact(&mut body_bytes)?;
    Ok(std::str::from_utf8(&body_bytes)?.to_string())
}

fn get_uri(header: &Vec<String>) -> Result<String, Box<dyn Error>> {
    let first = header.get(0).ok_or("Invalid header: no first line")?;
    let parts: Vec<_> = first.split(" ").collect();
    let uri = parts
        .get(1)
        .ok_or("Invalid header: Not enough stuff in first line of header")?;
    Ok((*uri).to_string())
}

fn get_content_type(header: &Vec<String>) -> Result<String, Box<dyn Error>> {
    for l in header {
        if l.starts_with("Content-Type") {
            let split: Vec<_> = l.split(":").collect();
            return Ok(split.get(1).ok_or("Invalid Content-Type header")?.trim().to_string());
        }
    }
    Err("No content type".into())
}

fn parse_request(stream: &mut TcpStream) -> Result<HttpRequest, Box<dyn Error>> {
    let mut buf_reader = BufReader::new(stream);
    let headers = read_header(&mut buf_reader)?;
    let uri = get_uri(&headers)?;
    let size = get_content_length(&headers)?;
    let body = get_body(&mut buf_reader, size)?;
    let uri_pieces: Vec<_> = uri.split("?").map(|s| s.to_string()).collect();
    let path = uri_pieces.get(0).ok_or("No path in request")?.to_string();
    let first_line: Vec<_> = headers.get(0).ok_or("No first line in header")?.split(" ").collect();
    let method = match first_line.get(0).ok_or("Empty first component")?.to_uppercase().as_str() {
        "POST"    => HttpMethod::Post,
        "GET"     => HttpMethod::Get,
        "PUT"     => HttpMethod::Put,
        "DELETE"  => HttpMethod::Delete,
        m         => {return Err(format!("Invalid method: '{m}'").into());},
    };
    let mut query_map = HashMap::<String,String>::new();
    if let Some(query) = uri_pieces.get(1) {
        for kv in query.split("&") {
            println!("kv = '{kv}'");
            if kv.is_empty() {
                continue
            }
            let split: Vec<_> = kv.split("=").collect();
            let k = split.get(0).ok_or(format!("Invalid query part: '{}'", kv))?;
            let v = split.get(1).ok_or(format!("Invalid query part: '{}'", kv))?;
            query_map.insert(k.to_string(), v.to_string());
        }
    }
    Ok(HttpRequest{
        method,
        path,
        query: query_map,
        headers,
        body,
    })
}

fn send_response(resp: &HttpResponse, stream: &mut TcpStream) -> Result<(), Box<dyn Error>> {
    stream.write_all(format!("HTTP/1.1 {} ", resp.code).as_bytes())?;
    if resp.code == 200 {
        stream.write_all("OK\r\n".as_bytes())?;
    } else {
        stream.write_all("ERROR\r\n".as_bytes())?;
    }
    for (k,v) in &resp.headers {
        stream.write_all(format!("{k}: {v}\r\n").as_bytes())?;
    }
    stream.write_all(format!("Content-Length: {}\r\n", resp.body.len()).as_bytes())?;
    stream.write_all("\r\n".as_bytes())?;
    stream.write_all(&resp.body)?;
    Ok(())
}

fn handle_connection(mut stream: TcpStream) -> Result<(), Box<dyn Error>> {
    let request = parse_request(&mut stream)?;
    if get_content_type(&request.headers)? == "application/json" {
        let sr: JobSpec = serde_json::from_str(&request.body)?;
        let job_id: u32;
        {
            // I think MutexGuard implements the deref trait so that
            // *mg is the u32 inside the MutexGuard which allows us to
            // change get or change the jobid.
            let mut mg = JOBID_COUNTER.lock()?;
            job_id = *mg;
            *mg += 1;
        }
        let job = Job{
            job_spec: sr,
            job_id,
            running: false,
            process: None,
        };
        println!("{:#?}", job);
        QUEUE.lock()?.push(job);
    }
    let mut resp = HttpResponse{
        code: 200,
        headers: HashMap::<String,String>::new(),
        body: request.body.as_bytes().to_owned(),
    };
    resp.headers.insert("Content-Type".to_string(), "application/json".to_string());
    send_response(&resp, &mut stream)?;
    Ok(())
}
