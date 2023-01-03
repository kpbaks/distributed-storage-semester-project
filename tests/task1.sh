#!/usr/bin/env bash

pwd


N=${1:-10}

k=${2:-2}

if ! [ -d ./log ]; then
    echo "Creating log directory"
    mkdir ./log
fi

for f in 10kB 100kB 1MB 10MB; do
    printf "POST ${f}\n"
    for t in task1.1 task1.2; do
        printf "${t}\n"
        logfile="log/${t}_post_${k}_${f}.txt"
        echo "time,time_replication,time_lead_total_work" > "${logfile}"
        for i in $(seq 1 $N); do
        # -r '[.filename, .content_type ] | @csv'
            http 192.168.0.101:9000/files_${t} -F < test_data/POST-request-${f}.json | jq -r '[.time, .time_replication, .time_lead_total_work] | @csv' >> log/${t}_post_${k}_${f}.txt
            printf "${i}/${N}\r"
        done
        printf "\n"
    done
    printf "\n"
done