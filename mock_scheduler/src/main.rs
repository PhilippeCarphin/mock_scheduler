use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::error::Error;
use std::io::prelude::*;
use std::io::BufReader;
use std::net::{TcpListener, TcpStream};
use std::result::Result;
use std::sync::mpsc::{channel, Receiver, Sender};

/*
 * TODO: Store jobs in a HashMap<job_id: u32, Job> inside a thread and make
 * the queue a Vec<u32> of job ids.
 *
 * - schedule() will pop a job ID from the queue and run the start command
 * TODO: A real scheduler would start all the jobs it can start if it had
 * infinite resources.  To simulate not having infinite resources, we can
 * imagine that we have like 4 "nodes" and running jobs would be running on
 * one of these "nodes".  A scheduling step would be something like
 * - Look if there is an available node
 * - If so, take a job from the queue and run it "on this node".
 * I'm not trying to make a real scheduler here, I just want a minimal fake
 * scheduler that I can submit fake jobs to for my maestro implementations.
 * Right now, in gomaestro, submission is simulated by just asynchronously
 * starting a subprocess.  This is inconvenient because the output of the
 * subprocess is in the same shell as the output of gomaestro which makes it
 * difficult to ascertain if everything is happening correctly.  It also means
 * that I can't be as realistic as I would like since instead of calling
 * ord_soumet, I'm calling bash.  This just allows me to have an ord_soumet
 * on my mac.
 */

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct JobSpec {
    script: String,
    #[serde(default = "Vec::<String>::new")]
    job_args: Vec<String>,
    #[serde(default = "HashMap::<String,String>::new")]
    submit_args: HashMap<String, String>,
}

#[allow(dead_code)]
#[derive(Debug)]
enum JobStatus {
    Submitted,
    Started,
    Finished,
    Aborted,
}

#[allow(dead_code)]
#[derive(Debug)]
struct Job {
    job_spec: JobSpec,
    job_id: u32,
    running: bool,
    status: JobStatus,
    process: Option<std::process::Child>,
}

impl Job {
    fn start(&mut self) -> Result<(), Box<dyn Error>> {
        self.process = Some(
            std::process::Command::new(&self.job_spec.script)
                .args(&self.job_spec.job_args)
                .spawn()?,
        );
        Ok(())
    }
    #[allow(dead_code)]
    fn pid(&self) -> Option<u32> {
        if let Some(child) = &self.process {
            Some(child.id())
        } else {
            None
        }
    }
    fn wait(&mut self) -> Result<std::process::ExitStatus, Box<dyn Error>> {
        if let Some(child) = &mut self.process {
            Ok(child.wait()?)
        } else {
            Err("This job has no process".into())
        }
    }
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
    method: HttpMethod,
    headers: Vec<String>, // TODO: Change to HashMap
    path: String,
    query: HashMap<String, String>,
    body: String, // Could be bytes but I'm only going to be doing strings
}
#[allow(dead_code)]
#[derive(Debug)]
struct HttpResponse {
    code: u32,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

struct JobStResp {}

struct JobDelResp {}

fn main() -> Result<(), Box<dyn Error>> {
    let (jobspec_tx, jobspec_rx): (Sender<JobSpec>, Receiver<JobSpec>) = channel();
    let (jobid_tx, jobid_rx): (Sender<u32>, Receiver<u32>) = channel();

    let (_jobst_tx, _jobst_rx): (Sender<u32>, Receiver<u32>) = channel();
    let (_jobdel_tx, _jobdel_rx): (Sender<u32>, Receiver<u32>) = channel();

    let (_jobst_resp_tx, _jobst_resp_rx): (Sender<JobStResp>, Receiver<JobStResp>) = channel();
    let (_jobdel_resp_tx, _jobdel_resp_rx): (Sender<JobDelResp>, Receiver<JobDelResp>) = channel();

    let (shutdown_tx, shutdown_rx): (Sender<u32>, Receiver<u32>) = channel();

    let scheduler_thread = std::thread::spawn(move || {
        let mut queue: Vec<Job> = Vec::<Job>::new();
        let mut jobid_counter: u32 = 0;
        loop {
            let job_spec = jobspec_rx.recv_timeout(std::time::Duration::from_secs(1));
            if let Ok(job_spec) = job_spec {
                let job_id: u32 = jobid_counter;
                jobid_counter += 1;
                let job = Job {
                    job_spec,
                    job_id,
                    status: JobStatus::Submitted,
                    running: false,
                    process: None,
                };
                queue.push(job);
                let _ = jobid_tx.send(job_id);
            }

            if let Err(e) = schedule(&mut queue) {
                println!("Error in schedule function: {e}, terminating thread");
                break;
            }
            std::thread::sleep(std::time::Duration::from_secs(5));
            if shutdown_rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .is_ok()
            {
                println!("Shutdown signal received on shutdown_rx in scheduler thread");
                return;
            }
        }
    });

    let listener = TcpListener::bind("127.0.0.1:7878")?;
    let _tcp_thread = std::thread::spawn(move || {
        for stream in listener.incoming() {
            if let Err(e) = handle_connection(
                stream.expect("Bad Stream"),
                &jobspec_tx,
                &jobid_rx,
                &shutdown_tx,
            ) {
                println!("Error handling connection: {}", e);
            } else {
                println!("Successfully handled connection!");
            }
        }
    });
    if let Err(e) = scheduler_thread.join() {
        return Err(format!("Could not join scheduler_tread: {:?}", e).into());
    }
    Ok(())
}

fn read_header(buf_reader: &mut BufReader<&mut TcpStream>) -> Result<Vec<String>, Box<dyn Error>> {
    let mut header: Vec<String> = vec![];
    loop {
        let mut s = String::new();
        buf_reader.read_line(&mut s)?;
        if s == "\r\n" {
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
            return Ok(split
                .get(1)
                .ok_or("Invalid Content-Type header")?
                .trim()
                .to_string());
        }
    }
    Err("No content type".into())
}

fn parse_request(stream: &mut TcpStream) -> Result<HttpRequest, Box<dyn Error>> {
    let mut buf_reader = BufReader::new(stream);
    let headers = read_header(&mut buf_reader)?;
    let uri = get_uri(&headers)?;
    let size = get_content_length(&headers).unwrap_or(0);
    let body = get_body(&mut buf_reader, size)?;
    let uri_pieces: Vec<_> = uri.split("?").map(|s| s.to_string()).collect();
    let path = uri_pieces.get(0).ok_or("No path in request")?.to_string();
    let first_line: Vec<_> = headers
        .get(0)
        .ok_or("No first line in header")?
        .split(" ")
        .collect();
    let method = match first_line
        .get(0)
        .ok_or("Empty first component")?
        .to_uppercase()
        .as_str()
    {
        "POST" => HttpMethod::Post,
        "GET" => HttpMethod::Get,
        "PUT" => HttpMethod::Put,
        "DELETE" => HttpMethod::Delete,
        m => {
            return Err(format!("Invalid method: '{m}'").into());
        }
    };
    let mut query_map = HashMap::<String, String>::new();
    if let Some(query) = uri_pieces.get(1) {
        for kv in query.split("&") {
            println!("kv = '{kv}'");
            if kv.is_empty() {
                continue;
            }
            let split: Vec<_> = kv.split("=").collect();
            let k = split
                .get(0)
                .ok_or(format!("Invalid query part: '{}'", kv))?;
            let v = split
                .get(1)
                .ok_or(format!("Invalid query part: '{}'", kv))?;
            query_map.insert(k.to_string(), v.to_string());
        }
    }
    Ok(HttpRequest {
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
    for (k, v) in &resp.headers {
        stream.write_all(format!("{k}: {v}\r\n").as_bytes())?;
    }
    stream.write_all(format!("Content-Length: {}\r\n", resp.body.len()).as_bytes())?;
    stream.write_all("\r\n".as_bytes())?;
    stream.write_all(&resp.body)?;
    Ok(())
}

fn handle_shutdown(mut stream: TcpStream, shutdown_tx: &Sender<u32>) -> Result<(), Box<dyn Error>> {
    println!("Shutdown request received!!!!!");
    let mut resp = HttpResponse {
        code: 200,
        headers: HashMap::<String, String>::new(),
        body: "Shutting down scheduler\n".as_bytes().to_owned(),
    };
    println!("Sending empty tuple on shutdown_tx");
    shutdown_tx.send(42).unwrap();
    println!("Empty tuple sent successfuly on shutdown_tx");
    resp.headers
        .insert("Content-Type".to_string(), "application/text".to_string());
    send_response(&resp, &mut stream)?;
    Ok(())
}

fn handle_submit(
    request: &HttpRequest,
    mut stream: TcpStream,
    jobspec_tx: &Sender<JobSpec>,
    jobid_rx: &Receiver<u32>,
) -> Result<(), Box<dyn Error>> {
    if get_content_type(&request.headers)? != "application/json" {
        return Err("Expected Content-Type: application/json".into());
    }
    println!("Request body = {}", request.body);
    let job_spec: JobSpec = serde_json::from_str(&request.body).map_err(|e| -> Box<dyn Error> {
        format!("Could not parse JSON: '{}': {}", request.body, e).into()
    })?;
    jobspec_tx.send(job_spec).unwrap();
    let job_id = jobid_rx.recv()?;
    // println!("{:#?}", job);
    let mut resp = HttpResponse {
        code: 200,
        headers: HashMap::<String, String>::new(),
        body: format!("{}", job_id).as_bytes().to_owned(),
    };
    resp.headers
        .insert("Content-Type".to_string(), "application/json".to_string());
    send_response(&resp, &mut stream)?;
    Ok(())
}

fn handle_connection(
    mut stream: TcpStream,
    jobspec_tx: &Sender<JobSpec>,
    jobid_rx: &Receiver<u32>,
    shutdown_tx: &Sender<u32>, /*, jobst_tx, jobstres_rx, jobdel_tx, jobdelres_rx, shutdown_tx*/
) -> Result<(), Box<dyn Error>> {
    let request = parse_request(&mut stream)?;
    match request.path.as_str() {
        "/shutdown" => handle_shutdown(stream, shutdown_tx),
        "/submit" => handle_submit(&request, stream, jobspec_tx, jobid_rx),
        _ => {
            let mut resp = HttpResponse {
                code: 200,
                headers: HashMap::<String, String>::new(),
                body: request.body.as_bytes().to_owned(),
            };
            resp.headers
                .insert("Content-Type".to_string(), "application/json".to_string());
            send_response(&resp, &mut stream)?;
            Ok(())
        }
    }
}

fn schedule(queue: &mut Vec<Job>) -> Result<(), Box<dyn Error>> {
    let job: Option<Job> = queue.pop();
    if let Some(mut job) = job {
        println!(
            "schedule(): Starting job (jobid = {})  {:#?}",
            job.job_id, job.job_spec
        );
        /*
         * NOTE: Job is no longer globally available, therefore a qstat
         * command would not be possible for a running job
         * NOTE: This way of running jobs only allows one job at a time to run
         * Perhaps a better way of doing things would be for this function to
         * take the job out of the queue, start it without waiting for it and
         * move in a container of running jobs.
         *
         * Then this function could do something like
         * - if running_jobs_container has less then max_running_jobs
         *       - Take a job from the queue, start it and put it in
         *         the running_jobs_container.
         * - Go through running_jobs_container, remove jobs whose process
         *   has ended
         * This way, qdel and qstat can be implemented
         * qdel:
         * - search in queue for job with jobid=<X> and remove if found
         * - search in running for job with jobid=<X>, kill process and remove
         *  if found
         * qstat:
         * - If no param, print a line for each job in queue, then one line for
         *   each job in running_jobs_container
         * - If jobid param, do the same search as for qdel and if a job is
         *   found, respond with some info about it.
         * NOTE: I'm starting to think that a better way of storing jobs would
         * be with a HashMap<u32, Job> and the queue could be a Vec<u32>,  THen
         * the qstat, qdel would be easy, they would find the job by JOBID in
         * the hashmap and the QUEUE would simply be used to decide in which
         * order to run the jobs.
         */
        if let Err(e) = job.start() {
            return Err(format!("Error starting job '{}': {e}", job.job_spec.script).into());
        }
        job.wait()?;
    }
    Ok(())
}
