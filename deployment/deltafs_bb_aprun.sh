#!/bin/bash
#
#MSUB -N deltafs-bb-test
#MSUB -l walltime=1:00:00
#MSUB -l nodes=10:haswell
#MSUB -o /users/$USER/joblogs/deltafs-bb-test-$MOAB_JOBID.out
#MSUB -j oe
##MSUB -V
##MSUB -m b
##MSUB -m $USER@lanl.gov


######################
# Tunable parameters #
######################

num_bbos_client_nodes=8
num_bbos_server_nodes=2
# num_bbos_client_nodes=8
# num_bbos_server_nodes=2

# Paths
umbrella_build_dir="$HOME/src/deltafs-umbrella/build"
deltafs_dir="$HOME/src/deltafs-bb"
output_dir="$HOME/src/deltafs_bb/dump" # SHOULD be burst buffer path
config_dir="$output_dir/config"
mkdir $config_dir
container_dir="$output_dir/containers"
mkdir $container_dir
rm -rf $container_dir/*
logfile=""
# umbrella_build_dir="/users/saukad/devel/deltafs-bb/build"
# deltafs_dir="/users/saukad/devel/deltafs-bb"
# output_dir="/tmp/bb"
# config_dir="$output_dir/config"
# mkdir $config_dir
# container_dir="$output_dir/containers"
# mkdir $container_dir
# rm -rf $container_dir/*
# logfile=""

###############
# Core script #
###############

OBJECT_CHUNK_SIZE[0]=1048576 # 1 MB
OBJECT_SIZE[0]=62914560 # 60 MB

OBJECT_CHUNK_SIZE[1]=2097152 # 2 MB
OBJECT_SIZE[1]=62914560 # 60 MB

OBJECT_CHUNK_SIZE[2]=4194304 # 4 MB
OBJECT_SIZE[2]=62914560 # 60 MB

OBJECT_CHUNK_SIZE[3]=15728640 # 15 MB
OBJECT_SIZE[3]=62914560 # 60 MB

PFS_CHUNK_SIZE[0] = 1048576
PFS_CHUNK_SIZE[1] = 2097152
PFS_CHUNK_SIZE[2] = 4194304
PFS_CHUNK_SIZE[3] = 8388608
PFS_CHUNK_SIZE[4] = 16777216

# OBJECT_CHUNK_SIZE=2
# OBJECT_SIZE=2048

message () { echo "$@" | tee $logfile; }
die () { message "Error $@"; exit 1; }

# Generate aprun -L comma-separated node lists
bbos_client_nodes = $(cat $PBS_NODEFILE | uniq | sort | head -n $num_bbos_client_nodes | tr '\n' ',')
bbos_server_nodes = $(cat $PBS_NODEFILE | uniq | sort | tail -n $num_bbos_server_nodes | tr '\n' ',')
# bbos_client_nodes="localhost"
# bbos_server_nodes="localhost"

# Get client and server IPs
message "Getting client IP addresses..."
aprun -L $bbos_client_nodes -n 1 -N 1 hostname -i > $output_dir/bbos_client_ip.txt
# mpirun --host $bbos_client_nodes hostname -i > $output_dir/bbos_client_ip.txt

message "Getting server IP addresses..."
aprun -L $bbos_server_nodes -n 1 -N 1 hostname -i > $output_dir/bbos_server_ip.txt
# mpirun --host $bbos_server_nodes hostname -i > $output_dir/bbos_server_ip.txt

bbos_client_path="$umbrella_build_dir/src/bbos_client"
bbos_server_path="$umbrella_build_dir/src/bbos_server"
# bbos_client_config_name="trinitite_client.conf"
# bbos_server_config_name="trinitite_server.conf"
bbos_client_config_name="narwhal_client.conf"
bbos_server_config_name="narwhal_server.conf"
bbos_client_config="$deltafs_dir/config/$bbos_client_config_name"
bbos_server_config="$deltafs_dir/config/$bbos_server_config_name"

# Now start servers and clients
server_num=0
declare -a server_pids
num_skip=0
num_bbos_clients_per_server=$(($num_bbos_client_nodes / $num_bbos_server_nodes))
p=0
for pfs_size in ${PFS_CHUNK_SIZE[@]}
do
  s=0
  for sizes in ${OBJECT_CHUNK_SIZE[@]}
  do
    for bbos_server in $(echo $bbos_server_nodes | sed "s/,/ /g")
    do
      # copying config files for every server
      new_server_config=$config_dir/$bbos_server_config_name.${OBJECT_CHUNK_SIZE[$s]}.${PFS_CHUNK_SIZE[$p]}.$bbos_server
      cp $bbos_server_config.${OBJECT_CHUNK_SIZE[$s]}.${PFS_CHUNK_SIZE[$p]} $new_server_config
      echo $bbos_server >> $new_server_config
      echo $container_dir >> $new_server_config
      aprun -L $bbos_server -n 2 -N 32 -d 2 $bbos_server_path $new_server_config 2>&1 | tee $logfile
      # mpirun --host $bbos_server $bbos_server_path $config_dir/$bbos_server_config_name.$bbos_server & 2>&1 | tee $logfile
      server_pids[$server_num]=$(echo $!)
      sleep 1

      message "Started BBOS server $bbos_server with PID ${server_pids[$server_num]}."
      j=0
      for bbos_client in $(echo $bbos_client_nodes | sed "s/,/ /g")
      do
        # skip first k nodes
        if [[ $j -lt $num_skip ]]; then
          j=$(($j + 1))
          n=0
          continue
        fi

        # copying config files for every client
        cp $bbos_client_config $config_dir/$bbos_client_config_name.$bbos_server
        echo $bbos_server >> $config_dir/$bbos_client_config_name.$bbos_server

        if [[ $n -lt $num_bbos_clients_per_server ]]; then
          object_name=$bbos_client-$bbos_server
          aprun -L $bbos_client -n 1 -N 1 -d 1 $bbos_client_path $object_name $config_dir/$bbos_client_config_name.$bbos_server ${OBJECT_SIZE[$s]} ${OBJECT_CHUNK_SIZE[$s]} 2>&1 | tee $logfile
          # mpirun --host $bbos_client $bbos_client_path $object_name $config_dir/$bbos_client_config_name.$bbos_server $OBJECT_SIZE $OBJECT_CHUNK_SIZE 2>&1 | tee $logfile
          message "Started BBOS client $bbos_client bound to BBOS server $bbos_server with object $object_name."
          sleep 1
          n=$(($n + 1))
        fi
      done
      num_skip=$(($num_skip + $num_bbos_clients_per_server))
      server_num=$(($server_num + 1))
    done
    i=0
    for bbos_server in $(echo $bbos_server_nodes | sed "s/,/ /g")
    do
      pid=${server_pids[$i]}
      message "Killing BBOS server on host $bbos_server"
      aprun -L $bbos_server -n 1 -N 1 -d 1 pkill -SIGINT bbos_server 2>&1 | tee $logfile
      # mpirun --host $bbos_server pkill -SIGINT bbos_server 2>&1 | tee $logfile
      i=$(($i + 1))
    done
    s=$(($s + 1))
  done
  p=$(($p + 1))
done
