#!/usr/bin/env bash

set -e

sbt ++$TRAVIS_SCALA_VERSION scalastyle
sbt ++$TRAVIS_SCALA_VERSION "test:scalastyle"
sbt ++$TRAVIS_SCALA_VERSION "it:scalastyle"

sbt \
  -Daws.testVersion=$AWS_JAVA_SDK_VERSION \
  -Dhadoop.testVersion=$HADOOP_VERSION \
  -Dspark.testVersion=$SPARK_VERSION \
  ++$TRAVIS_SCALA_VERSION \
  coverage test coverageReport

if [ "$TRAVIS_SECURE_ENV_VARS" == "true" ]; then
  sbt \
    -Daws.testVersion=$AWS_JAVA_SDK_VERSION \
    -Dhadoop.testVersion=$HADOOP_VERSION \
    -Dspark.testVersion=$SPARK_VERSION \
    ++$TRAVIS_SCALA_VERSION \
    coverage it:test coverageReport 2> /dev/null;
fi
