#!/usr/bin/env bash

./mvnw -pl '!:trino-server-rpm,!:trino-docs,!:trino-proxy,!:trino-verifier,!:trino-benchto-benchmarks' clean install \
    -T2 -nsu -P disable-check-spi-dependencies \
    -DskipTests -Dmaven.javadoc.skip=true -Dmaven.source.skip=true -Dair.check.skip-all=true -Dskip.npm -Dskip.yarn
