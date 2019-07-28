package io.qkits.udf.hive;

import io.qkits.udf.hive.utils.SsnTool;
import io.qkits.udf.hive.utils.StringUtil;

import org.apache.hadoop.hive.ql.exec.UDF;


public class SsnAge extends UDF {

  public Integer evaluate(String ssn) {
    if (ssn == null) {
      return null;
    } else {
      int ret = SsnTool.getAge(SsnTool.cleanSsn(ssn));
      if (ret == -1) {
        return null;
      } else {
        return ret;
      }
    }
  }
}
