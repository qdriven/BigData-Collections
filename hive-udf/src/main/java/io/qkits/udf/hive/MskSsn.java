package io.qkits.udf.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import io.qkits.udf.hive.utils.SsnTool;
import io.qkits.udf.hive.utils.StringUtil;

public class MskSsn extends UDF {

  public String evaluate(String ssn) {
    if (ssn == null || ssn.isEmpty()) {
      return ssn;
    }

    int spl = ssn.lastIndexOf("_");
    String pre_ssn, post_ssn;
    if (spl < 0) {
      pre_ssn = "";
      post_ssn = ssn;
    } else {
      pre_ssn = ssn.substring(0, spl + 1);
      post_ssn = ssn.substring(spl + 1);
    }

    if (SsnTool.isValidSSN(post_ssn).isEmpty()) {
      return ssn;
    }
    if (post_ssn.length() == 15) {
      // return pre_ssn + post_ssn.substring(0, 12) + "**" + post_ssn.substring(14, 15);
      return post_ssn.substring(0, 12) + "**" + post_ssn.substring(14, 15);
    }
    if (post_ssn.length() == 18) {
      // return pre_ssn + post_ssn.substring(0, 14) + "**" + post_ssn.substring(16, 17) + "*";
      return post_ssn.substring(0, 14) + "**" + post_ssn.substring(16, 17) + "*";
    }
    return ssn;
  }
}
