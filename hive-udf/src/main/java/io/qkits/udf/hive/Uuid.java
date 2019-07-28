package io.qkits.udf.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.UUID;


public class Uuid extends UDF {

  public String evaluate() {
    return UUID.randomUUID().toString();
  }
}

