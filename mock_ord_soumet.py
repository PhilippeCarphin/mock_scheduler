import argparse
import requests
import sys
import json
import os

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
submit_args = p.parse_args(submit_args)

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
    'submit_args': submit_args.__dict__
}
r = requests.post("http://localhost:7878/submit", json.dumps(request_dict), headers={"Content-Type": "application/json"})

