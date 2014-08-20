import sys
import subprocess
import os

MAX_ITER = 1

#GRAB all data shards in DATA Folder
#python multipass.py mapper.py reducer.py
mapper_file  = sys.argv[1] 
reducer_file = sys.argv[2]
data_shard_filenames =  os.listdir("DATA")

#python mapper DATA_FILE, MAP_INPUT, iteration_NUM
#python reducer MAP_OUTPUT, MAP_INPUT, iteration_NUM
#reducer decides when to stop the iterations

for i in xrange(0,MAX_ITER):
		
	worker_processes = []
	#spawn mapper processes (one per data shard)
	for idx, file_name in enumerate(data_shard_filenames):
		sys_args = ["python", mapper_file, file_name, str(i)]
		p = subprocess.Popen(sys_args)
		worker_processes.append(p)

	#wait for processes to finish
	mapper_exit_codes = [p.wait() for p in worker_processes]

	#spawn reducer process and wait for it to finish	
	p = subprocess.Popen(["python", reducer_file])
	reduce_exit_code = p.wait()

	#reducer has signalled convergence
	print reduce_exit_code	
	if reduce_exit_code == 1:
		break
