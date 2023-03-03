#!/bin/bash

# set default values for optional arguments
# Default interval is 5 minutes
interval=300
# The log files all be compressed into .tar.gz
compress=0
iterations=0

# parse command line arguments
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    -i|--interval)
    interval="$2"
    shift # past argument
    shift # past value
    ;;
    -f|--folder)
    folder="$2"
    shift # past argument
    shift # past value
    ;;
    -c|--compress)
    compress=1
    shift # past argument
    ;;
    *)    # unknown option
    shift # past argument
    ;;
esac
done

# Run the log collection after every interval seconds
while true
do
    /bin/bash /cluster/logger/collect_log.sh $iterations $compress $folder
    sleep $interval
    iterations=$(($iterations+1))
done
