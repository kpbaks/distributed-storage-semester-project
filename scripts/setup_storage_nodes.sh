#!/usr/bin/env bash

# 96a2dc92-2d30-48fb-95c1-95509e152fde
# 87c0c255-fe2c-4cde-9011-dcc2137d933c
# 13d7d596-28d1-4f88-8716-1f0df2639c67
# 4b2f2b3f-6f88-43ab-8210-5c60d038f74e

declare -A NODES

NODES[storage0]="96a2dc92-2d30-48fb-95c1-95509e152fde"
NODES[storage1]="87c0c255-fe2c-4cde-9011-dcc2137d933c"
NODES[storage2]="13d7d596-28d1-4f88-8716-1f0df2639c67"
NODES[storage3]="4b2f2b3f-6f88-43ab-8210-5c60d038f74e"


for folder in "${!NODES[@]}"; do
    echo "Creating folder $folder"
    mkdir -p $folder
    echo "Creating file $folder/.node_id"
    echo ${NODES[$folder]} > $folder/.node_id
    sqlite3 files.db "INSERT INTO storage_nodes (uid, address, port) VALUES ('${NODES[$folder]}', 'localhost', 5555);"
done

