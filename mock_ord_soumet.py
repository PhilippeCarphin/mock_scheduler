import argparse
import requests
import sys
import json
import os

scheduler_host = "localhost"
scheduler_port = 7878

if len(sys.argv) < 2:
    print("ERROR: This tool requires at least one argument")
    sys.exit(1)

#
# Intercept shutdown request
#
if sys.argv[1] == '-s':
    r = requests.post("http://localhost:7878/shutdown")
    sys.exit(0)


argv = sys.argv[1:]
script = argv.pop(0)
print(f"script = {script}")
if script == "-":
    # TODO: read stdin into a temporary file and replace script with path of
    #       that file.
    pass
if not os.path.isfile(script):
    print("ERROR: First argument must be the path to an executable binary or script")
    sys.exit(1)
script = os.path.realpath(script)

#
# Grab arguments up to '--' as arguments for the scheduler
#
submit_args = []
while argv:
    a = argv.pop(0)
    if a == "--":
        break;
    submit_args.append(a)
p = argparse.ArgumentParser()
p.add_argument("-cpu", help="Number of cores")
p.add_argument("-m", help="Memory")
p.add_argument("-w", help="Wallclock")
submit_args_namespace = p.parse_args(submit_args)
submit_args = {}
for k,v in submit_args_namespace.__dict__.items():
    if v is not None:
        submit_args[k] = v

#
# The arguments after '--' are arguments meant to be passed to the script
# or binary being run in the job.
#
job_args = argv

#
# Compose an object that will be sent as JSON
#
request_dict = {
    'script': script,
    'job_args': job_args,
    'submit_args': submit_args
}
r = requests.post(f"http://{scheduler_host}:{scheduler_port}/submit", json.dumps(request_dict, indent=4), headers={"Content-Type": "application/json"})
print(r.text)

