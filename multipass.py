import sys
import subprocess


MAX_ITER = 1000

#GRAB all data shards in DATA Folder
#python multipass.py mapper.py reducer.py

print sys.argv[1], sys.argv[2]


#iteration count passed as param
#python mapper DATA_FILE, MAP_INPUT, iteration_NUM
#python reducer MAP_OUTPUT, MAP_INPUT, iteration_NUM
#reducer decides when to stop the iterations

for i in xrange(0,MAX_ITER):
	
	#spawn mapper processes (32 on AWS machines)
	worker_processes = []
	p1 = subprocess.Popen("ls")
	p2 = subprocess.Popen("ls")
	worker_processes.append(p1)
	worker_processes.append(p2)


	#wait for processes to finish
	mapper_exit_codes = [p.wait() for p in worker_processes]

	#spawn reducer process
	
	p1 = subprocess.Popen(["ls","-alih"])
	reduce_exit_code = p1.wait()
