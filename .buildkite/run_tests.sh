#!/usr/bin/env bash
set -eu

# remove old containers
echo "+++ :docker: Cleanup"
docker-compose -p $BUILDKITE_BUILD_ID rm --force

# run tests
echo "+++ :elixir: Tests"
docker-compose -p $BUILDKITE_BUILD_ID run --rm $REPO_NAME
