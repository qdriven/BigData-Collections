#!/usr/bin/env bash

SPARK_HOME="~/workspace/spark-2.3.3-bin-hadoop2.7"

${SPARK_HOME}/bin/spark-submit --class "io.qkits.spark.DQSparkApp" \
        target/spark-template-1.0-SNAPSHOT.jar "select * from data_connector where isIdCardNo(ERRORNUM) !=0"

