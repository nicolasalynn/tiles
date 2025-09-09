#!/bin/bash
#PBS -N tiles_backend_persistent
#PBS -l walltime=180:00:00   # total runtime of job (1 hour here)
#PBS -l nodes=1:ppn=1
#PBS -j oe
#PBS -o repeat_job.out
#PBS -q tamirQ
#PBS -l mem=7gb

cd /tamir2/nicolaslynn/projects/tiles

while true; do
    echo "Running at $(date)" >> log.txt
    python b44_interface.py
    sleep 30
done