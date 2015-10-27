#!/usr/bin/env bash

set -e

#sbt ++$TRAVIS_SCALA_VERSION scalastyle
#sbt ++$TRAVIS_SCALA_VERSION "test:scalastyle"
#sbt ++$TRAVIS_SCALA_VERSION "it:scalastyle"

#sbt \
  #-Dhadoop.testVersion=$HADOOP_VERSION \
  #-Dspark.testVersion=$SPARK_VERSION \
  #++$TRAVIS_SCALA_VERSION \
  #coverage test

if [ "$TRAVIS_SECURE_ENV_VARS" == "true" ]; then
  sbt \
    -Dhadoop.testVersion=$HADOOP_VERSION \
    -Dspark.testVersion=$SPARK_VERSION \
    ++$TRAVIS_SCALA_VERSION \
    coverage "it:testOnly *RedshiftIntegration* -- -z \"roundtrip save and load\""
    # coverage it:test 2> /dev/null;
fi
