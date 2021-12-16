#!/bin/bash
echo "creating output directory"
mkdir -p output
echo "compiling core"
cd core/
javac *.java -d ../output
echo "compiling indicators"
cd ../indicators/
javac -cp ../output/ *.java -d ../output
echo "compiling jl"
cd ../jl/
javac -cp ../output/ *.java -d ../output
echo "compiling gui"
cd ../gui/
javac -cp ../output/ *.java -d ../output
cd ..
