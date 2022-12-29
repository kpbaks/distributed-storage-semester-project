#!/bin/bash

for i in {0..3}; do echo "storage-node$i:" && cat ../storage-node$i/* && echo; done
