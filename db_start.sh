#!/bin/bash

set -e

source prepare-shell.sh

LOCAL_DB_DATA=`readlink -m "$TEMP_DIR/arangodb_data"`
CONTAINER_ARANGODB_DATA=/var/lib/arangodb3
DOCKER_IMAGE_NAME=arangodb
DOCKER_INSTANCE_NAME=citations-arangodb
DB_ROOT_PASSWORD=
STORAGE_ENGINE=auto

echo "using: $LOCAL_DB_DATA"

EXISTING_INSTANCES=`docker ps -a -f name=$DOCKER_INSTANCE_NAME -q`

if [ ! -z "$EXISTING_INSTANCES" ]; then
  echo "removing existing instance: $EXISTING_INSTANCES"
  docker rm -f $EXISTING_INSTANCES
  sleep 1
fi

if [ "$1" == "--clean" ]; then
  echo "removing $LOCAL_DB_DATA"
  sudo rm -rf "$LOCAL_DB_DATA"
fi

mkdir -p "$LOCAL_DB_DATA"

echo "starting instance $DOCKER_INSTANCE_NAME, using storage engine $STORAGE_ENGINE at $LOCAL_DB_DATA"
docker run \
  --name "$DOCKER_INSTANCE_NAME" \
  -e ARANGO_NO_AUTH=1 \
  -e ARANGO_ROOT_PASSWORD=$DB_ROOT_PASSWORD \
  -e ARANGO_STORAGE_ENGINE=$STORAGE_ENGINE \
  -p 8529:8529 -d \
  -v $LOCAL_DB_DATA:$CONTAINER_ARANGODB_DATA \
  $DOCKER_IMAGE_NAME \
  arangod

sleep 10

docker logs "$DOCKER_INSTANCE_NAME"
