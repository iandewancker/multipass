import sys
import subprocess






#wait for processes to finish
exit_codes = [p.wait() for p in worker_processes]
