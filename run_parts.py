import os
import shlex
import subprocess

if not os.path.exists('logs'):
    os.mkdir('logs')

ct = 0

procs = []

for filename in os.listdir('indices'):
    logf = open('logs/{}.log'.format(ct))
    proc = subprocess.Popen(shlex.split('go run wikite.go -badFile bad/{}.txt -inFile indices/{}'.format(ct, filename)),
                            stdout=logf, stderr=logf)

    procs.append(proc)

    ct += 1

for proc in procs:
    proc.wait()
    proc.stdout.close()
