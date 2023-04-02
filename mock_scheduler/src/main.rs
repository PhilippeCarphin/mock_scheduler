use std::error::Error;
use std::io::prelude::*;
use std::io::BufReader;
use std::net::{TcpListener, TcpStream};
use std::result::Result;
/*
 * This test demonstrates a simple HTTP server handling requests and reading
 * the body.
 *
 * The tutorial in the Rust Book showed a simple way to read the header of the
 * request by reading from a TCP stream until an empty line and then returning
 * some HTML.
 *
 * However it took me a while to figure out how to read the request body
 * because I was trying things like buf_reader.read_to_string(&s).  This would
 * block because I did not understand that the stream itself does not know
 * when it ends.
 *
 * We need to get the content length out of the header and read that many bytes
 * from the stream.
 *
 * The process is as follows, with buf_reader = BufReader::new(&mut stream):
 * - Use buf_reader.read_line(&s) to create a Vec<String>.  This stores the
 *   header in a form that is easy to manipulate.
 *   This is the function get_header
 * - Iterate over the Vec<String> header to find the Content-Lenght.
 * - Read exactly that number of bytes from the stream (buf_reader).
 *
 */

fn main() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:7878").unwrap();

    for stream in listener.incoming() {
        let stream = stream.unwrap();

        handle_connection(stream)?;
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

fn handle_connection(mut stream: TcpStream) -> Result<(), Box<dyn Error>> {
    let mut buf_reader = BufReader::new(&mut stream);
    let header = read_header(&mut buf_reader)?;
    let uri = get_uri(&header)?;
    let size = get_content_length(&header)?;
    let body = get_body(&mut buf_reader, size)?;
    println!("HEADER: {:#?}", header);
    let uri_pieces: Vec<_> = uri.split("?").map(|s| s.to_string()).collect();
    let path = uri_pieces.get(0).ok_or("No path in request")?;
    let query = uri_pieces.get(1);
    println!("PATH: {path}");
    println!("QUERY: {:?}", query);
    println!("{}", body);
    // Just making sure length returns the number length in bytes which seems
    // to be the case.  I tested by sending a request containing emojis and
    // accented characters so doing 'as_bytes' is not necessary.
    println!("byte_length = {}, string_length = {}", body.as_bytes().len(), body.len());
    stream
        .write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/text\r\nContent-Length: {}\r\n\r\n{}\r\n",
                body.len()+2,
                body
            )
            .as_bytes(),
        )
        .unwrap();
    Ok(())
}
