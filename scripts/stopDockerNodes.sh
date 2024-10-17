#!/bin/bash   

if [ "$#" -ne 2 ]; then
    echo "$0 <nNodes> {<host1>,...,<hostn>}"
    exit 1
fi

nNodes=$1
hostsArg="${2#\{}"  # Remove leading {
hostsArg="${hostsArg%\}}"  # Remove trailing }
IFS=',' read -r -a hosts <<< "$hostsArg"

max=$(( ${#hosts[@]} - 1 ))
s=0

for i in $(seq 1 $nNodes) 
do
	name=asd-$i
	server=${hosts[$s]}
	echo $name $server
	echo "ssh $server \"docker stop $name\""
	ssh $server "docker stop $name"	

	s=$(( $s + 1 ))
	if [ $s -gt $max ]; then
		s=0
	fi  
done

