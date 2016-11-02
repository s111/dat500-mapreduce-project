#!/bin/sh

rm -r classes 2> /dev/null
rm messageinputformat.jar 2> /dev/null

mkdir classes

javac -classpath `hadoop classpath` -d classes *.java
jar -cvf messageinputformat.jar -C classes/ .
