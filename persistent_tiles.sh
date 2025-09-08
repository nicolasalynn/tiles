#!/bin/bash
#PBS -N repeat_job
#PBS -l walltime=01:00:00   # total runtime of job (1 hour here)
#PBS -l nodes=1:ppn=1
#PBS -j oe
#PBS -o repeat_job.out

cd $PBS_O_WORKDIR

while true; do
    echo "Running at $(date)" >> log.txt
    python b44_interface.py
    sleep 30
done