#!/bin/bash

declare -a CHUNK_SERVERS=(
  "nashville"
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
  "loveland"
  "steamboat"
)

for CHUNK_SERVER in "${CHUNK_SERVERS[@]}"
do
  echo -e "Deleting stored chunks on ${CHUNK_SERVER} (/tmp/menukaw)"
  ssh "${CHUNK_SERVER}" "rm -rf /tmp/menukaw"
done
