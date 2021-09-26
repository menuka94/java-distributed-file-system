#!/bin/bash

declare -a CHUNK_SERVERS=(
  "lattice-10"
  "lattice-11"
  "lattice-12"
  "lattice-14"
  "lattice-16"
  "boston" # clear the /tmp/menukaw dir of client
  "nashville"
#  "salt-lake-city"
#  "cheyenne"
#  "helena"
#  "lincoln"
#  "nashville"
#  "phoenix"
#  "providence"
)

for CHUNK_SERVER in "${CHUNK_SERVERS[@]}"
do
  echo -e "Deleting stored chunks on ${CHUNK_SERVER} (/tmp/menukaw)"
  ssh "${CHUNK_SERVER}" "rm -rf /tmp/menukaw"
done
