#!/bin/bash

set -e

DIR="$(realpath $(dirname $0))"

cleanup() {
  cd ${DIR}/../conf/docker
  RUNNING_CONTAINERS=$(docker-compose ps -q)
  for CONTAINER_NAME in ${RUNNING_CONTAINERS}
  do
    echo "Stopping: ${CONTAINER_NAME}"
    docker stop ${CONTAINER_NAME}
    echo "Container stopped: ${CONTAINER_NAME}"
  done
  echo "Removing dead containers"
  local CONTAINERS=`docker ps -aq --no-trunc --filter status=dead --filter status=exited`
  for CONTAINER in ${CONTAINERS};
  do
    docker rm -v "${CONTAINER}"
  done
}

run_in_application_runner_container() {
     local CONTAINER_NAME=$( docker-compose -f ${DIR}/../conf/docker/docker-compose.yml run -d controller "$@" )
     echo "Showing logs from $CONTAINER_NAME:"
     docker logs -f $CONTAINER_NAME
     return $( docker inspect --format '{{.State.ExitCode}}' $CONTAINER_NAME )
}

ENVIRONMENT=$1

docker-compose version
docker version

#compile & generate JAR
cd ${DIR}/../..
mvn clean install



docker-compose -f ${DIR}/../conf/docker/docker-compose.yml up -d --force-recreate

#run test

set +e

java -jar ${DIR}/../../presto-manager-tests/target/Test.jar

TEST_PROCESS_ID=$!
wait ${TEST_PROCESS_ID}
EXIT_CODE=$?

set -e

cleanup

exit ${EXIT_CODE}
