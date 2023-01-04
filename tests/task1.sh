#!/usr/bin/env bash

pwd

trap 'kill $(jobs -p); exit 1' SIGINT


N=${1:-10}

TOTAL_REQUESTS=$((N*2*4))

k=${2:-2}

printf "REQUEST per test=%d, TOTAL_REQUESTS=%d, k=%d\n" ${N} ${TOTAL_REQUESTS} ${k}

if ! [ -d ./log ]; then
    echo "Creating log directory"
    mkdir ./log
fi

idx=0

# print a growing line


for f in 10kB 100kB 1MB 10MB; do
    printf "POST ${f}\n"
    for t in task1.1 task1.2; do
        printf "  ${t}\n"
        logfile="log/${t}_post_${k}_${f}.txt"
        echo "time,time_replication,time_lead_total_work" > "${logfile}"
        for i in $(seq 1 ${N}); do
        # -r '[.filename, .content_type ] | @csv'
            http 192.168.0.101:9000/files_${t} -F < test_data/POST-request-${f}.json | jq -r '[.time, .time_replication, .time_lead_total_work] | @csv' >> log/${t}_post_${k}_${f}.txt
            idx=$((idx+1))
            percentage=$((100*idx/TOTAL_REQUESTS))
            printf "    ${idx}/${TOTAL_REQUESTS} [${percentage}%]\r"
        done
        printf "\n"
    done
    printf "\n"
done