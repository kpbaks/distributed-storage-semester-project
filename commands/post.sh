#!/bin/bash
PATH+=:/usr/bin

python3 ../create_post_request.py $1 | http POST localhost:9000/files

