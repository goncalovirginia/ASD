#!/bin/bash   

if [ "$#" -ne 1 ]; then
	echo "$0 {<host1>,...,<hostn>}"
	exit 1
fi

hostsArg="${1#\{}"  # Remove leading {
hostsArg="${hostsArg%\}}"  # Remove trailing }
IFS=',' read -r -a nodes <<< "$hostsArg"
image=asd
path=$(pwd)
nNodes=${#nodes[@]}

for i in $(seq 0 $(($nNodes - 1))) 
do
	server=${nodes[$i]}
	echo "updating $server..."
	ssh $server "cd \"$path\" && docker build -t $image ."
done
