#!/bin/bash
# run client
CP="dist/pa3.jar:.:lib/log4j-1.2.16.jar:lib/commons-lang3-3.1.jar:lib/commons-lang-2.3.jar:lib/commons-configuration-1.8.jar:lib/commons-logging-1.1.1.jar"
MAIN="pa3.Server"

# start client
java -cp $CP $MAIN


