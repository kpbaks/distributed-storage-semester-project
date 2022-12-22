#!/usr/bin/env bash

dependencies=(sqlite3,python3)

for cmd in ${dependencies[@]}; do
	if ! command -v $cmd &> /dev/null
	then
		echo "$cmd could not be found"
		exit
	fi
done

if [ ! -f "files.db" ]; then
	echo "Creating database..."
	sqlite3 files.db < ./create_table.sql
fi


