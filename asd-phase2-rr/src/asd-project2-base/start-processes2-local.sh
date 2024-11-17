#!/bin/bash

#More complete script that starts 4 replicas in the initial membership
#And all the other replicas join the system.
#The initial membership will add themselves following the initial script logic
#The other replicas will add with an interval of 5s between each other
#should be more than enough time to get some flow before joining


processes=$1

if [ -z $processes ] || [ $processes -lt 1 ]; then
  echo "please indicate a number of processes of at least one"
  exit 0
fi

i=0
base_p2p_port=34000
base_server_port=35000

membership="localhost:${base_p2p_port}"

read -p "------------- Press enter start. After starting, press enter to kill all servers --------------------"

i=1
while [ $i -lt $processes ]; do
    if [ $i -lt 3 ]; then
      membership="${membership},localhost:$(($base_p2p_port + $i))"
    fi
    i=$(($i + 1))
done

i=0
while [ $i -lt $processes ]; do
  
  if [ $i -gt 0 ] && [ $i -lt 3 ]; then
    sleep 1
  elif [ $i -eq 3 ]; then
    sleep 3
    echo "-------------------- Initial Membership launched --------------------"
    sleep 6
  else
    sleep 5
  fi

  java -DlogFilename=logs/node$(($base_p2p_port + $i)) -cp target/asdProj2.jar Main -conf config.properties address=localhost p2p_port=$(($base_p2p_port + $i)) server_port=$(($base_server_port + $i)) initial_membership=$membership 2>&1 | sed "s/^/[$(($base_p2p_port + $i))] /" &
  echo "launched process on p2p port $(($base_p2p_port + $i)), server port $(($base_server_port + $i))"

  i=$(($i + 1))
done

#kill $(ps aux | grep 'asdProj2.jar' | awk '{print $2}')

#echo "All processes done!"
