#!/bin/bash
mkdir $1
cd $1
cat "../template.py" > "$1.py"
atom "$1.py"
echo "$1/$1.py successfully created"
