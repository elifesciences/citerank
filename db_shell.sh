#!/bin/bash

set -e

source prepare-shell.sh

DOCKER_INSTANCE_NAME=citations-arangodb

docker exec -it "$DOCKER_INSTANCE_NAME" bash
