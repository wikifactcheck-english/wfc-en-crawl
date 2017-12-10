import os
import shlex
import subprocess

if not os.path.exists('logs'):
    os.mkdir('logs')

ct = 0

procs = []
files = []

env = os.environ
env['GOMAXPROCS'] = '4'

for filename in os.listdir('indices'):
    logf = open('logs/{}.log'.format(ct), 'w')
    proc = subprocess.Popen(shlex.split('go run cmd/refdl/refdl.go -badFile bad/{}.txt -indexFile indices/{}'.format(ct, filename)),
                            stdout=logf, stderr=logf, env=env)

    procs.append(proc)
    files.append(logf)

    ct += 1

for proc in procs:
    proc.wait()

for file in files:
    file.close()
