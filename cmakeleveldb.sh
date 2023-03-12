#!/bin/bash
cd /home/jjia/code/leveldb
rm -rf build
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Debug .. && cmake --build .

