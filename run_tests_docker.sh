#!/usr/bin/env bash

MIX_ENV=test mix do local.rebar --force, local.hex --force,  deps.get
for ((i=0; i < 10; i++)); do
    curl ${RABBITMQ_HOST}:15672 > /dev/null 2>&1
    if [ $? -eq 0 ]; then
	break
    fi
    sleep 1
    echo "Waiting for rabbitmq..."
done

## Run the tests
mix test
