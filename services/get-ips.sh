#!/bin/bash

docker-compose ps | cut -d " " -f 1 | grep -v ^$ | grep -v "\-\-" | xargs -L 1 docker inspect --format '{{ .NetworkSettings.Networks.default_network.IPAddress Name }}' $1

