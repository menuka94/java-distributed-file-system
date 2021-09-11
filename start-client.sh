#!/bin/bash

JAR_FILE=../../../libs/cs555-hw1-1.0.jar
CLASSES_DIR=build/classes/java/main

# Start Client on boston
cd ${CLASSES_DIR} && java -cp ${JAR_FILE} cs555.hw1.node.Client arkansas 9000
