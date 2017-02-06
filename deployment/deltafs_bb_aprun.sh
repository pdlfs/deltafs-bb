#!/bin/bash
#
#MSUB -N deltafs-bb-test
#MSUB -l walltime=1:00:00
#MSUB -l nodes=5:haswell
#MSUB -o /users/$USER/joblogs/deltafs-test-$MOAB_JOBID.out
#MSUB -j oe
##MSUB -V
##MSUB -m b
##MSUB -m $USER@lanl.gov


######################
# Tunable parameters #
######################

num_bbos_client_nodes=128
num_bbos_server_nodes=4
# num_bbos_client_nodes=8
# num_bbos_server_nodes=2

# Paths
umbrella_build_dir="$HOME/src/deltafs-umbrella/build"
deltafs_dir="$HOME/src/deltafs_bb"
output_dir="$HOME/src/deltafs_bb/dump"
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

PBS_NODEFILE_SERVER="servers.txt"
PBS_NODEFILE_CLIENT="clients.txt"

OBJECT_CHUNK_SIZE=2097152
OBJECT_SIZE=10737418240

message () { echo "$@" | tee $logfile; }
die () { message "Error $@"; exit 1; }

# Generate aprun -L comma-separated node lists
bbos_client_nodes=$(cat $PBS_NODEFILE_CLIENT | uniq | sort | head -n $num_bbos_client_nodes | tr '\n' ',')
bbos_server_nodes=$(cat $PBS_NODEFILE_SERVER | uniq | sort | head -n $num_bbos_server_nodes | tr '\n' ',')
# bbos_client_nodes="c1,c2,c3,c4,c5,c6,c7,c8"
# bbos_server_nodes="s1,s2"

# Get client and server IPs
message "Getting client IP addresses..."
aprun -L $bbos_client_nodes -n 1 -N 1 hostname -i > $output_dir/bbos_client_ip.txt

message "Getting server IP addresses..."
aprun -L $bbos_server_nodes -n 1 -N 1 hostname -i > $output_dir/bbos_server_ip.txt

bbos_client_path="$umbrella_build_dir/src/bbos_client"
bbos_server_path="$umbrella_build_dir/src/bbos_server"
bbos_client_config_name="trinitite_client.conf"
bbos_server_config_name="trinitite_server.conf"
bbos_client_config="$deltafs_dir/config/$bbos_client_config_name"
bbos_server_config="$deltafs_dir/config/$bbos_server_config_name"

# Now start servers and clients
num_skip=0
num_bbos_clients_per_server=$(($num_bbos_client_nodes / $num_bbos_server_nodes))
for bbos_server in $(echo $bbos_server_nodes | sed "s/,/ /g")
do
  # copying config files for every server
  cp $bbos_server_config $config_dir/$bbos_server_config_name.$bbos_server
  aprun -L $bbos_server -n 2 -N 32 -d 2 $bbos_server_path $config_dir/$bbos_server_config_name.$bbos_server 2>&1 | tee $logfile
  $bbos_server-pid=$!

  message "Started BBOS server $bbos_server."
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
      object_name=$bbos_client$bbos_server
      aprun -L $bbos_client -n 1 -N 1 -d 1 $bbos_client_path $object_name $config_dir/$bbos_client_config_name.$bbos_server $OBJECT_CHUNK_SIZE $OBJECT_SIZE 2>&1 | tee $logfile
      n=$(($n + 1))
      message "Started BBOS client $bbos_client bound to BBOS server $bbos_server."
    fi
  done
  num_skip=$(($num_skip + $num_bbos_clients_per_server))
done

for bbos_server in $(echo $bbos_server_nodes | sed "s/,/ /g")
do
  kill -SIGINT $bbos_server-pid
done
