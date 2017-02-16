#!/bin/bash
#
#MSUB -N deltafs-bb-test
#MSUB -l walltime=1:00:00
#MSUB -l nodes=35:haswell
#MSUB -o /users/$USER/joblogs/deltafs-bb-test-$MOAB_JOBID.out
#MSUB -j oe
##MSUB -V
##MSUB -m b
##MSUB -m $USER@lanl.gov


######################
# Tunable parameters #
######################

num_bbos_client_nodes=30
num_bbos_server_nodes=5

# Paths
umbrella_build_dir="$HOME/src/deltafs-umbrella/build"
deltafs_dir="$HOME/src/deltafs-bb"
output_dir="$HOME/src/deltafs-bb/dump" # SHOULD be within burst buffer mount point
config_dir="$output_dir/config"
mkdir $config_dir
logfile=""
# umbrella_build_dir="/users/saukad/devel/deltafs-bb/build"
# deltafs_dir="/users/saukad/devel/deltafs-bb"
# output_dir="/tmp/bb"
# config_dir="$output_dir/config"
# mkdir $config_dir
# logfile=""

###############
# Core script #
###############

# Client transfer sizes we experiment with
OBJECT_CHUNK_SIZE[0]=1048576 # 1 MB
OBJECT_SIZE[0]=1073741824 # 1 GB

OBJECT_CHUNK_SIZE[1]=2097152 # 2 MB
OBJECT_SIZE[1]=1073741824 # 1 GB

OBJECT_CHUNK_SIZE[2]=4194304 # 4 MB
OBJECT_SIZE[2]=1073741824 # 1 GB

OBJECT_CHUNK_SIZE[3]=8388608 # 8 MB
OBJECT_SIZE[3]=1073741824 # 1 GB

# PFS sizes we experiment with
#PFS_CHUNK_SIZE[0]=1048576
#PFS_CHUNK_SIZE[1]=2097152
#PFS_CHUNK_SIZE[2]=4194304
PFS_CHUNK_SIZE[0]=8388608
PFS_CHUNK_SIZE[1]=16777216
PFS_CHUNK_SIZE[2]=33554432
PFS_CHUNK_SIZE[3]=67108864

# OBJECT_CHUNK_SIZE=2
# OBJECT_SIZE=2048

message () { echo "$@" | tee $logfile; }
die () { message "Error $@"; exit 1; }

# Generate aprun -L comma-separated node lists
# PBS_NODEFILE="fakenodes.txt"
bbos_client_nodes=$(cat $PBS_NODEFILE | uniq | sort | head -n $num_bbos_client_nodes | tr '\n' ',')
bbos_server_nodes=$(cat $PBS_NODEFILE | uniq | sort | tail -n $num_bbos_server_nodes | tr '\n' ',')

bbos_client_path="$umbrella_build_dir/bin/bbos_client"
bbos_server_path="$umbrella_build_dir/bin/bbos_server"
# bbos_client_path="$umbrella_build_dir/src/bbos_client"
# bbos_server_path="$umbrella_build_dir/src/bbos_server"
bbos_client_config_name="trinitite_client.conf"
bbos_server_config_name="trinitite_server.conf"
# bbos_client_config_name="narwhal_client.conf"
# bbos_server_config_name="narwhal_server.conf"
bbos_client_config="$deltafs_dir/config/$bbos_client_config_name"
bbos_server_config="$deltafs_dir/config/$bbos_server_config_name"

# Now start servers and clients
num_bbos_clients_per_server=$(($num_bbos_client_nodes / $num_bbos_server_nodes))
p=0
for pfs_size in ${PFS_CHUNK_SIZE[@]}
do
  message ""
  message "========= TESTING FOR DW CHUNK SIZE ${PFS_CHUNK_SIZE[$p]} ========="
  dd=0
  s=0
  for sizes in ${OBJECT_CHUNK_SIZE[@]}
  do
    message ""
    message "========= TESTING FOR MERCURY TRANSFER SIZE ${OBJECT_CHUNK_SIZE[$s]} ========="
    server_num=0
    declare -a server_pids
    num_skip=0
    n=0
    container_dir=$output_dir/containers-${OBJECT_CHUNK_SIZE[$s]}-${PFS_CHUNK_SIZE[$p]}
    mkdir $container_dir
    for bbos_server in $(echo $bbos_server_nodes | sed "s/,/ /g")
    do
      if [[ "$dd" == 0 ]]; then
        # create a dummy file and test raw throughput to DW using dd
        num_pfs_chunks_per_container=$((503316480 / ${PFS_CHUNK_SIZE[$p]})) # 480 MB / PFS_CHUNK_SIZE
        message "Checking RAW throughput using dd for PFS_CHUNK_SIZE of ${PFS_CHUNK_SIZE[$p]} and container size of 480 MB"
        aprun -L $bbos_server -n 1 -N 1 -d 1 dd if=/dev/urandom of=$container_dir/dummy.file.$bbos_server bs=${PFS_CHUNK_SIZE[$p]} count=$num_pfs_chunks_per_container 2>&1 | tee $logfile
        # mpirun --host $bbos_server dd if=/dev/urandom of=$container_dir/dummy.file bs=${PFS_CHUNK_SIZE[$p]} count=$num_pfs_chunks_per_container 2>&1 | tee $logfile
        dd=1
      fi

      # copying config files for every server
      new_server_config=$config_dir/$bbos_server_config_name.${OBJECT_CHUNK_SIZE[$s]}.${PFS_CHUNK_SIZE[$p]}.$bbos_server
      cp $bbos_server_config.${OBJECT_CHUNK_SIZE[$s]}.${PFS_CHUNK_SIZE[$p]} $new_server_config
      echo $bbos_server >> $new_server_config
      echo $container_dir >> $new_server_config
      aprun -L $bbos_server -n 2 -N 32 -d 2 $bbos_server_path $new_server_config & 2>&1 | tee $logfile # not put in background on purpose
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
          aprun -L $bbos_client -n 1 -N 1 -d 1 $bbos_client_path $object_name $config_dir/$bbos_client_config_name.$bbos_server ${OBJECT_SIZE[$s]} ${OBJECT_CHUNK_SIZE[$s]} & 2>&1 | tee $logfile
          # mpirun --host $bbos_client $bbos_client_path $object_name $config_dir/$bbos_client_config_name.$bbos_server $OBJECT_SIZE $OBJECT_CHUNK_SIZE 2>&1 | tee $logfile
          message "Started BBOS client $bbos_client bound to BBOS server $bbos_server with object $object_name."
          sleep 1
          n=$(($n + 1))
        fi
      done
      num_skip=$(($num_skip + $num_bbos_clients_per_server))
      server_num=$(($server_num + 1))
    done
    message "Sleeping for clients to finish writing..."
    sleep 100
    i=0
    for bbos_server in $(echo $bbos_server_nodes | sed "s/,/ /g")
    do
      pid=${server_pids[$i]}
      # killing servers to perform final binpackings
      message "Killing BBOS server on host $bbos_server"
      aprun -L $bbos_server -n 1 -N 1 -d 1 pkill -SIGINT bbos_server 2>&1 | tee $logfile
      # mpirun --host $bbos_server pkill -SIGINT bbos_server 2>&1 | tee $logfile
      i=$(($i + 1))
    done
    s=$(($s + 1))
  done
  p=$(($p + 1))
done
