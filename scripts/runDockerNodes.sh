#!/bin/bash   

if [ "$#" -ne 3 ]; then
    echo "$0 <resultFolderID> <nNodes> {<host1>,...,<hostn>}"
    exit 1
fi


resultsDir=/home/$(whoami)/results/$i
if [ ! -d $resultsDir ]; then
  mkdir $resultsDir && echo "created $resultsDir"
fi

nNodes=$2
hostsArg="${3#\{}"  # Remove leading {
hostsArg="${hostsArg%\}}"  # Remove trailing }
IFS=',' read -r -a hosts <<< "$hostsArg"

swarmManager=${hosts[0]}

cpu=1
net=asd2025
bandwidth=100
image=asd

jarDir=/home/$(whoami)/jars
if [ ! -d $jarDir ]; then
  mkdir $jarDir && echo "created $jarDir"
fi

IFS=$'\n' read -d '' -r -a ips < ./ips200.txt

max=$(( ${#hosts[@]} - 1 ))

ssh $swarmManager "docker network rm $net --force"
ssh $swarmManager "docker network create $net -d overlay --attachable --subnet 10.10.0.0/16 --gateway 10.10.0.1"

s=0

for i in $(seq 1 $nNodes) 
do
	name=asd-$i
	ip=${ips[$i-1]}
	server=${hosts[$s]}
	echo $name $ip $server
	if [ ! -d ${resultsDir}/${name} ]; then
	  mkdir ${resultsDir}/${name} && echo "created ${resultsDir}/${name}"
	fi
	
	echo "ssh $server \"$docker run --rm -d -t --cpus=$cpu --privileged -v $jarDir:/home/asd/jars -v $resultsDir/$name:/home/asd/logs -v /lib/modules:/lib/modules --cap-add=ALL --net $net --ip $ip --name $name --hostname $name $image $i $bandwidth\""
	ssh $server "docker run --rm -d -t --cpus=$cpu --privileged -v $jarDir:/home/asd/jars -v $resultsDir/$name:/home/asd/logs -v /lib/modules:/lib/modules --cap-add=ALL --net $net --ip $ip --name $name --hostname $name $image $i $bandwidth"

	s=$(( $s + 1 ))
	if [ $s -gt $max ]; then
		s=0
	fi  
done

