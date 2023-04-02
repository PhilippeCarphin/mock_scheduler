#!/usr/bin/env python3
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import time
import subprocess

#
# Basic scheduler prototype.  It listens for HTTP requests for requests
# containing a script to run.
#
# It is single threaded and uses a loop to check for requests and run jobs.
#
# Because I'm a Rustacean now, the main implementation is the rust one but I
# still want to keep this one functionnal and compatible with my ord_soumet
# tool.
#

keep_going = True

class Job:
    def __init__(self, job_spec, jobid):
        self.running = False
        self.jobid = jobid
        self.process = None
        self.job_spec = job_spec
    def start(self):
        try:
            self.process = subprocess.Popen([self.job_spec.script, *self.job_spec.args])
        except Exception as e:
            print(f"OOPSIE POOPSIE: {e}")
            return False
        return True
    def __str__(self):
        pid = self.process.pid if self.process else None
        return f"JOBID: {self.jobid}, PID: {pid}, SCRIPT: {self.job_spec.script}, JOB_ARGS={self.job_spec.args}, SUBMIT_ARGS: {self.job_spec.submit_args}"

class JobSpec:
    def __init__(self, script, **kwargs):
        self.script = script
        self.args = kwargs.get('args', [])
        self.submit_args = kwargs.get('submit_args', [])
        self.kwargs = kwargs

class Scheduler:
    jobid_counter = 0
    def __init__(self):
        self.queue = []
        self.running_jobs = []
        self.current_job = None
    def submit(self, job_spec):
        self.jobid_counter += 1
        new_job = Job(job_spec, self.jobid_counter)
        self.queue.append(new_job)
        return new_job
    def delete(self, job_id):
        pass
    def schedule(self):
        if self.current_job is None:
            if self.queue:
                self.current_job = self.queue.pop(0)
                print(f"==== Starting job {self.current_job}")
                if not self.current_job.start():
                    self.current_job = None
        else:
            poll = self.current_job.process.poll()
            if poll is not None:
                print(f"==== Job {self.current_job} finished")
                self.current_job = None

class APIHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == '/submit':
            self.handle_submit()
        elif self.path == '/delete':
            self.handle_delete()
        elif self.path == '/status':
            self.handle_status()
        elif self.path == '/shutdown':
            self.handle_shutdown()
        else:
            self.send_response(404)
    def get_request_body_json(self):
        content_length = int(self.headers['Content-Length'])
        return json.loads(self.rfile.read(content_length))
    def handle_submit(self):
        request_body = self.get_request_body_json()
        new_job = SCHEDULER.submit(
            JobSpec(
                script=request_body['script'],
                args=request_body['job_args'],
                submit_args=request_body['submit_args']
            )
        )
        output = f"Jobid is {new_job.jobid}".encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/text')
        self.end_headers()
        self.wfile.write(output)
    def handle_shutdown(self):
        global keep_going
        keep_going = False
        self.send_response(200)
        self.send_header('Content-Type', 'application/text')
        self.end_headers()
        self.wfile.write(b'Shutdown acknowledged\n')

SCHEDULER = Scheduler()
handler = APIHandler
httpd = HTTPServer(('localhost', 7878), handler)
httpd.timeout = 1

try:
    while keep_going:
        httpd.handle_request()
        SCHEDULER.schedule()
        time.sleep(1)
except KeyboardInterrupt:
    print("")
    pass

