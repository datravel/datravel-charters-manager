#!/bin/sh -ex

DOCKER_OUTPUT=$(docker build .)
echo $DOCKER_OUTPUT

DOCKER_ID=$(echo "$DOCKER_OUTPUT" | tail -n 1 | grep '^Successfully built' | awk '{print $3}')

SSH_AUTH_SOCK_PATH=$(dirname $SSH_AUTH_SOCK)

docker run -v $(pwd):/build -v $SSH_AUTH_SOCK_PATH:$SSH_AUTH_SOCK_PATH -e SSH_AUTH_SOCK=$SSH_AUTH_SOCK -e UID=$(id -u) $DOCKER_ID