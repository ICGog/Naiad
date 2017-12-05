#!/bin/bash
for i in `seq 1 10` ; do
  cd ~/data-artisans-ycsb
  ./stream-bench.sh STOP_REDIS
  ./stream-bench.sh START_REDIS
  cd ~/falkirk/Naiad/
  /home/srguser/falkirk-experiments/scripts/run_naiad_ycsb_25_machines.sh
  cd ~/data-artisans-ycsb
  ./stream-bench.sh STOP_LOAD
  ./stream-bench.sh STOP_REDIS
  cd ~/falkirk/Naiad/
  mv ~/data-artisans-ycsb/data/latencies.txt latencies-10000k-25machines-6-threads-1000ms-slice-nonincremental-exactly-once-naiad-$i.txt
done

for i in `seq 1 10` ; do
  cd ~/data-artisans-ycsb
  ./stream-bench.sh STOP_REDIS
  ./stream-bench.sh START_REDIS
  cd ~/falkirk/Naiad/
  /home/srguser/falkirk-experiments/scripts/run_naiad_ycsb_failure_25_machines.sh
  cd ~/data-artisans-ycsb
  ./stream-bench.sh STOP_LOAD
  ./stream-bench.sh STOP_REDIS
  cd ~/falkirk/Naiad/
  mv ~/data-artisans-ycsb/data/latencies.txt latencies-10000k-25machines-6-threads-1000ms-slice-nonincremental-exactly-once-failure-naiad-$i.txt
done

for i in `seq 1 10` ; do
  cd ~/data-artisans-ycsb
  ./stream-bench.sh STOP_REDIS
  ./stream-bench.sh START_REDIS
  cd ~/falkirk/Naiad/
  /home/srguser/falkirk-experiments/scripts/run_naiad_ycsb_8_threads_25_machines.sh
  cd ~/data-artisans-ycsb
  ./stream-bench.sh STOP_LOAD
  ./stream-bench.sh STOP_REDIS
  cd ~/falkirk/Naiad/
  mv ~/data-artisans-ycsb/data/latencies.txt latencies-10000k-25machines-8-threads-1000ms-slice-nonincremental-exactly-once-naiad-$i.txt
done
