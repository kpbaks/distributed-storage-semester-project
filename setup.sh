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
	storage1
	storage2
	storage3
	storage4
)

for node in "${storage_nodes[@]}"; do
	# sleep 2
	echo "Creating folder $node"
	if [ -d "$node" ]; then
		echo "Folder $node already exists"
		rm -rf "$node"
	fi
	mkdir -p "$node"
done


if [ -f "files.db" ]; then
	echo "Database already exists"
	rm files.db
fi
echo "Creating database..."
sqlite3 files.db < ./create_table.sql

# if ! [ -f "files.db" ]; then
# fi

# if ! [ -f "messages_pb2.py" ]; then
echo "Generating classes from ./messages.proto"
protoc --python_out=. messages.proto
# fi