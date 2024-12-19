#!/bin/bash

echo "build mydbt docker image ..."
docker build images/mydbt -t mydbt

echo "build mysoda docker image ..."
docker build images/mysoda -t mysoda

echo "build custom python docker image"
docker build images/mypython -t mypython

docker compose up -d