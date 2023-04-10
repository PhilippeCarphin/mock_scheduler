#!/usr/bin/python3

import requests

scheduler_host = "localhost"
scheduler_port = 7878

resp = requests.get(f"http://{scheduler_host}:{scheduler_port}/status&jobid={jobid}")

job_status = resp.json()

