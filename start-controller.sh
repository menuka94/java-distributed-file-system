#!/bin/bash

JAR_FILE=../../../libs/cs555-hw1-1.0.jar
CLASSES_DIR=build/classes/java/main

# Start Controller
cd ${CLASSES_DIR} && java -cp ${JAR_FILE} cs555.hw1.Controller 9000
