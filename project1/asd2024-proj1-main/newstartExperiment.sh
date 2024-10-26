#!/bin/bash   

if [ "$#" -ne 3 ]; then
    echo "$0 <nNodes> <jar> {<host1>,...,<hostn>}"
    exit 1
fi

nNodes=$1
jar="../jars/$2"
hostsArg="${3#\{}"  # Remove leading {
hostsArg="${hostsArg%\}}"  # Remove trailing }
IFS=',' read -r -a hosts <<< "$hostsArg"

# You should change this following commands to start your process
cmdfirst="cd logs && java -cp $jar Main port=10101 processSequence=1 2>&1 > console.log"
cmd="cd logs && java -cp $jar Main interface=eth0 port=10101 contact=10.10.53.231:10101 processSequence=<INDEX> 2>&1 > console.log"

max=$(( ${#hosts[@]} - 1 ))
s=0


for i in $(seq 1 $nNodes) 
do
	name=asd-$i
	server=${hosts[$s]}
	echo $name $ip $server
	
	if [ $i -eq 1 ]; then
		c=$cmdfirst
	else
		c=${cmd/<INDEX>/$i}
	fi

	echo "ssh $server \"docker exec -dt $name sh -c '$c'\""  
	ssh $server "docker exec -dt $name sh -c '$c'"

	s=$(( $s + 1 ))
	if [ $s -gt $max ]; then
		s=0
	fi  

	sleep 1s
done
