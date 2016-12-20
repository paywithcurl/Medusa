#!/usr/bin/env bash
set -eu
docker-compose -p $BUILDKITE_BUILD_ID down
docker-compose -p $BUILDKITE_BUILD_ID kill
