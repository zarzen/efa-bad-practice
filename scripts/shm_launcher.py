import argparse
import subprocess
from subprocess import Popen, PIPE
import threading
import time
import atexit
import select

def clean_shm(prefix):
  subprocess.call("rm /dev/shm/"+prefix + "*", shell=True)
  subprocess.call("rm /dev/shm/sem."+prefix + "-*", shell=True)

def check_output(proc, name):
  print(name, "check output")
  y = select.poll()
  y.register(proc.stdout, select.POLLIN)
  while not proc.poll():
    if y.poll(1):
      print(proc.stdout.readline())
    else:
      time.sleep(1)

def clean_proc(processes):
  for p in processes:
    p.kill()


def main():
  """
  """
  parser = argparse.ArgumentParser()
  parser.add_argument('--ip', type=str, default="0.0.0.0", help="IP")
  parser.add_argument('--port', type=str, default='8888')
  parser.add_argument('--mode', type=str, required=True)
  parser.add_argument('--comm-name', type=str, required=True)
  parser.add_argument('--num-workers', type=str, default='3')
  parser.add_argument("--data-buf-name", type=str, required=True)
  parser.add_argument('--data-buf-size', type=str, default='1000000000') # 1e9

  args = parser.parse_args()

  clean_shm(args.comm_name)
  # open communicator
  if args.mode in ('c', "client"):
    comm_exe = "shm_cli_comm"
  else:
    comm_exe = "shm_serv_comm"
  comm_cmd = "./{} {} {} {} {} {}".format(comm_exe, 
                                          args.ip, args.port, 
                                          args.comm_name, args.num_workers, 
                                          args.data_buf_name,
                                          args.data_buf_size)

  comm_p = Popen(comm_cmd, shell=True)

  nw = int(args.num_workers)
  workers = []
  for i in range(nw):
    rank = str(i)
    w_p = Popen(["./shm_worker", args.comm_name, args.num_workers, rank, 
                args.data_buf_name,
                args.data_buf_size])
    workers.append(w_p)

  atexit.register(clean_proc, workers+[comm_p])
  atexit.register(clean_shm, args.shm_prefix)

  while not comm_p.poll():
    time.sleep(1)

if __name__ == "__main__":
    main()