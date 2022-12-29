#!/bin/bash

rm ../files.db

cat ../create_table.sql | sqlite3 ../files.db

rm ../storage-node{0..3}/*
