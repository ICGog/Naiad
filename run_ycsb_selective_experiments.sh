#!/bin/bash

# IMPORTANT: Ensure ptp is setup in your cluster so that times are as synced as possible.

# for i in `seq 1 10` ; do
#   cd ~/data-artisans-ycsb
#   ./stream-bench.sh STOP_REDIS
#   ./stream-bench.sh START_REDIS
#   cd ~/falkirk/Naiad/
#   /home/srguser/falkirk-experiments/scripts/run_naiad_ycsb_selective_60000k_delay_at_30_exactly_once_25_machines.sh
#   cd ~/data-artisans-ycsb
#   ./stream-bench.sh STOP_LOAD
#   ./stream-bench.sh STOP_REDIS
#   cd ~/falkirk/Naiad/
#   mv ~/data-artisans-ycsb/data/latencies.txt latencies-selective-60000k-25machines-4-threads-1000ms-slice-nonincremental-exactly-once-naiad-delay-at-30-$i.txt
# done

# git checkout Naiad/Dataflow/Endpoints.cs
# parallel-scp -h ~/caelum_configs/caelum_all Naiad/Dataflow/Endpoints.cs /home/srguser/falkirk/Naiad/Naiad/Dataflow/Endpoints.cs
# parallel-ssh -h ~/caelum_configs/caelum_all -t 0 -i 'cd falkirk/Naiad; xbuild /p:Configuration="MonoRelease"'

# for i in `seq 1 10` ; do
#   cd ~/data-artisans-ycsb
#   ./stream-bench.sh STOP_REDIS
#   ./stream-bench.sh START_REDIS
#   cd ~/falkirk/Naiad/
#   /home/srguser/falkirk-experiments/scripts/run_naiad_ycsb_nonselective_60000k_delay_at_30_exactly_once_25_machines.sh
#   cd ~/data-artisans-ycsb
#   ./stream-bench.sh STOP_LOAD
#   ./stream-bench.sh STOP_REDIS
#   cd ~/falkirk/Naiad/
#   mv ~/data-artisans-ycsb/data/latencies.txt latencies-nonselective-60000k-25machines-4-threads-1000ms-slice-nonincremental-exactly-once-naiad-delay-at-30-$i.txt
# done


# for i in `seq 1 10` ; do
#   cd ~/data-artisans-ycsb
#   ./stream-bench.sh STOP_REDIS
#   ./stream-bench.sh START_REDIS
#   cd ~/falkirk/Naiad/
#   /home/srguser/falkirk-experiments/scripts/run_naiad_ycsb_selective_delay_at_30_duplicate_25_machines.sh
#   cd ~/data-artisans-ycsb
#   ./stream-bench.sh STOP_LOAD
#   ./stream-bench.sh STOP_REDIS
#   cd ~/falkirk/Naiad/
#   mv ~/data-artisans-ycsb/data/latencies.txt latencies-selective-120000k-25machines-4-threads-1000ms-slice-naiad-delay-at-30-$i.txt
# done


# git checkout Naiad/Dataflow/Endpoints.cs
# parallel-scp -h ~/caelum_configs/caelum_all Naiad/Dataflow/Endpoints.cs /home/srguser/falkirk/Naiad/Naiad/Dataflow/Endpoints.cs
# parallel-ssh -h ~/caelum_configs/caelum_all -t 0 -i 'cd falkirk/Naiad; xbuild /p:Configuration="MonoRelease"'

# for i in `seq 1 10` ; do
#   cd ~/data-artisans-ycsb
#   ./stream-bench.sh STOP_REDIS
#   ./stream-bench.sh START_REDIS
#   cd ~/falkirk/Naiad/
#   /home/srguser/falkirk-experiments/scripts/run_naiad_ycsb_nonselective_delay_at_30_duplicate_25_machines.sh
#   cd ~/data-artisans-ycsb
#   ./stream-bench.sh STOP_LOAD
#   ./stream-bench.sh STOP_REDIS
#   cd ~/falkirk/Naiad/
#   mv ~/data-artisans-ycsb/data/latencies.txt latencies-nonselective-120000k-25machines-4-threads-1000ms-slice-naiad-delay-at-30-$i.txt
# done


# for i in `seq 1 10` ; do
#   cd ~/data-artisans-ycsb
#   ./stream-bench.sh STOP_REDIS
#   ./stream-bench.sh START_REDIS
#   cd ~/falkirk/Naiad/
#   /home/srguser/falkirk-experiments/scripts/run_naiad_ycsb_selective_delay_8_epochs_duplicate_25_machines.sh
#   cd ~/data-artisans-ycsb
#   ./stream-bench.sh STOP_LOAD
#   ./stream-bench.sh STOP_REDIS
#   cd ~/falkirk/Naiad/
#   mv ~/data-artisans-ycsb/data/latencies.txt latencies-selective-120000k-25machines-4-threads-1000ms-slice-naiad-delay-8-epochs-$i.txt
# done


# git checkout Naiad/Dataflow/Endpoints.cs
# parallel-scp -h ~/caelum_configs/caelum_all Naiad/Dataflow/Endpoints.cs /home/srguser/falkirk/Naiad/Naiad/Dataflow/Endpoints.cs
# parallel-ssh -h ~/caelum_configs/caelum_all -t 0 -i 'cd falkirk/Naiad; xbuild /p:Configuration="MonoRelease"'

# for i in `seq 1 10` ; do
#   cd ~/data-artisans-ycsb
#   ./stream-bench.sh STOP_REDIS
#   ./stream-bench.sh START_REDIS
#   cd ~/falkirk/Naiad/
#   /home/srguser/falkirk-experiments/scripts/run_naiad_ycsb_nonselective_delay_8_epochs_duplicate_25_machines.sh
#   cd ~/data-artisans-ycsb
#   ./stream-bench.sh STOP_LOAD
#   ./stream-bench.sh STOP_REDIS
#   cd ~/falkirk/Naiad/
#   mv ~/data-artisans-ycsb/data/latencies.txt latencies-nonselective-120000k-25machines-4-threads-1000ms-slice-naiad-delay-8-epochs-$i.txt
# done


# for i in `seq 1 10` ; do
#   cd ~/data-artisans-ycsb
#   ./stream-bench.sh STOP_REDIS
#   ./stream-bench.sh START_REDIS
#   cd ~/falkirk/Naiad/
#   /home/srguser/falkirk-experiments/scripts/run_naiad_ycsb_selective_delay_5_epochs_duplicate_25_machines.sh
#   cd ~/data-artisans-ycsb
#   ./stream-bench.sh STOP_LOAD
#   ./stream-bench.sh STOP_REDIS
#   cd ~/falkirk/Naiad/
#   mv ~/data-artisans-ycsb/data/latencies.txt latencies-selective-120000k-25machines-4-threads-1000ms-slice-naiad-delay-5-epochs-$i.txt
# done


# git checkout Naiad/Dataflow/Endpoints.cs
# parallel-scp -h ~/caelum_configs/caelum_all Naiad/Dataflow/Endpoints.cs /home/srguser/falkirk/Naiad/Naiad/Dataflow/Endpoints.cs
# parallel-ssh -h ~/caelum_configs/caelum_all -t 0 -i 'cd falkirk/Naiad; xbuild /p:Configuration="MonoRelease"'

# for i in `seq 1 10` ; do
#   cd ~/data-artisans-ycsb
#   ./stream-bench.sh STOP_REDIS
#   ./stream-bench.sh START_REDIS
#   cd ~/falkirk/Naiad/
#   /home/srguser/falkirk-experiments/scripts/run_naiad_ycsb_nonselective_delay_5_epochs_duplicate_25_machines.sh
#   cd ~/data-artisans-ycsb
#   ./stream-bench.sh STOP_LOAD
#   ./stream-bench.sh STOP_REDIS
#   cd ~/falkirk/Naiad/
#   mv ~/data-artisans-ycsb/data/latencies.txt latencies-nonselective-120000k-25machines-4-threads-1000ms-slice-naiad-delay-5-epochs-$i.txt
# done

for i in `seq 1 10` ; do
  cd ~/data-artisans-ycsb
  ./stream-bench.sh STOP_REDIS
  ./stream-bench.sh START_REDIS
  cd ~/falkirk/Naiad/
  /home/srguser/falkirk-experiments/scripts/run_naiad_ycsb_selective_delay_5_epochs_exactly_once_25_machines.sh
  cd ~/data-artisans-ycsb
  ./stream-bench.sh STOP_LOAD
  ./stream-bench.sh STOP_REDIS
  cd ~/falkirk/Naiad/
  mv ~/data-artisans-ycsb/data/latencies.txt latencies-selective-60000k-25machines-4-threads-1000ms-slice-exactly-once-naiad-delay-5-epochs-$i.txt
done


git checkout Naiad/Dataflow/Endpoints.cs
parallel-scp -h ~/caelum_configs/caelum_all Naiad/Dataflow/Endpoints.cs /home/srguser/falkirk/Naiad/Naiad/Dataflow/Endpoints.cs
parallel-ssh -h ~/caelum_configs/caelum_all -t 0 -i 'cd falkirk/Naiad; xbuild /p:Configuration="MonoRelease"'

for i in `seq 1 10` ; do
  cd ~/data-artisans-ycsb
  ./stream-bench.sh STOP_REDIS
  ./stream-bench.sh START_REDIS
  cd ~/falkirk/Naiad/
  /home/srguser/falkirk-experiments/scripts/run_naiad_ycsb_nonselective_delay_5_epochs_exactly_once_25_machines.sh
  cd ~/data-artisans-ycsb
  ./stream-bench.sh STOP_LOAD
  ./stream-bench.sh STOP_REDIS
  cd ~/falkirk/Naiad/
  mv ~/data-artisans-ycsb/data/latencies.txt latencies-nonselective-60000k-25machines-4-threads-1000ms-slice-exactly-once-naiad-delay-5-epochs-$i.txt
done
