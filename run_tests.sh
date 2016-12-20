#!/usr/bin/env bash

docker run --name medusa-rabbitmq -p 5672:5672 -p 15672:15672 -d rabbitmq:3.6.5-management

for ((i=0; i < 10; i++)); do
    curl 127.0.0.1:15672 > /dev/null 2>&1
    if [ $? -eq 0 ]; then
	break
    fi
    echo "Waiting for rabbitmq..."
    sleep 1
done

## Run the tests
mix test

docker rm -f medusa-rabbitmq
