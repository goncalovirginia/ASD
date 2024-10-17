#!/bin/bash   

if [ "$#" -ne 2 ]; then
    echo "Illegal number of parameters, must indicate number of processes"
    exit 1
fi

hostsArg="${2#\{}"  # Remove leading {
hostsArg="${hostsArg%\}}"  # Remove trailing }
IFS=',' read -r -a nodes <<< "$hostsArg"

max=$(( ${#nodes[@]} - 1 ))
s=0


for i in $(seq 1 $1) 
do
	name=asd-$i
	server=${nodes[$s]}
	echo $name $ip $server
	
	echo "\"ssh $server docker exec -t $name sh -c  'killall -SIGINT java'\""  
	ssh $server "docker exec -t $name sh -c 'killall -SIGINT java'"

	s=$(( $s + 1 ))
	if [ $s -gt $max ]; then
		s=0
	fi  
done
