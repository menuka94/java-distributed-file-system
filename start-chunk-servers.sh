#!/bin/bash

JAR_FILE=../../../libs/cs555-hw1-1.0.jar
CLASSES_DIR=cs555-hw1/build/classes/java/main

declare -a CHUNK_SERVERS=(
  "denver"
  "boise"
  "sacramento"
  "salt-lake-city"
  "cheyenne"
  "helena"
  "lincoln"
  "phoenix"
  "providence"
  "columbia"
  "jackson"
  "madison"
  "little-rock"
  "olympia"
  "berlin"
  "cairo"
  "lima"
  "london"
)

for CHUNK_SERVER in "${CHUNK_SERVERS[@]}"
do
  xterm -hold -title "ChunkServer: ${CHUNK_SERVER}"  -e "ssh -t ${CHUNK_SERVER} \"cd ${CLASSES_DIR} && java -cp ${JAR_FILE} cs555.hw1.node.chunkServer.ChunkServer arkansas 9000\"" &
done