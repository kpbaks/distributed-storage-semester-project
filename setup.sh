#!/usr/bin/env bash

# dependencies=(sqlite3,python3,protoc)

# for cmd in ${dependencies[@]}; do
# 	if ! command -v "$cmd" &> /dev/null
# 	then
# 		echo "$cmd could not be found"
# 		exit
# 	fi
# done

# declare -A NODES

declare -a storage_nodes=(
	storage0
	storage1
	storage2
	storage3
)

for node in "${storage_nodes[@]}"; do
	sleep 2
	echo "Creating folder $node"
	if [ -d "$node" ]; then
		echo "Folder $node already exists"
		rm -rf "$node"
	fi
	mkdir -p "$node"
done

# NODES[storage0]="06a2dc92-2d30-48fb-95c1-95509e152fde"
# NODES[storage1]="17c0c255-fe2c-4cde-9011-dcc2137d933c"
# NODES[storage2]="23d7d596-28d1-4f88-8716-1f0df2639c67"
# NODES[storage3]="3b2f2b3f-6f88-43ab-8210-5c60d038f74e"


if [ -f "files.db" ]; then
	echo "Database already exists"
	rm files.db
fi
echo "Creating database..."
sqlite3 files.db < ./create_table.sql


# for folder in "${!NODES[@]}"; do
#     echo "Creating folder $folder"
# 	if [ -d "$folder" ]; then
# 		echo "Folder $folder already exists"
# 		rm -rf "$folder"
# 	fi
#     mkdir -p "$folder"
#     # echo "Creating file $folder/.node_id"
#     # echo "${NODES[$folder]}" > "$folder"/.node_id
#     # sqlite3 files.db "INSERT INTO storage_nodes (uid, address, port) VALUES ('${NODES[$folder]}', 'localhost', 5580);"
# done




# if ! [ -f "files.db" ]; then
# fi

# if ! [ -f "messages_pb2.py" ]; then
echo "Generating classes from ./messages.proto"
protoc --python_out=. messages.proto
# fi