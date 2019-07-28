package io.qkits.udf.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.qkits.udf.hive.utils.StringUtil;

public class MskEmail extends UDF {

  static Pattern
      patternEmail =
      Pattern.compile("^(\\w+(\\.\\w+)*)@((\\w+(-\\w+)?\\.)+[a-zA-Z]{2,})$");

  public String evaluate(String email) {
    if (email == null || email.isEmpty()) {
      return email;
    }
    Matcher m = patternEmail.matcher(email);
    if (!m.find()) {
      return email;
    }
    String name = m.group(1);
    String domain = m.group(3);

    return name.substring(0, name.length() / 2) + StringUtil
        .repeat("*", name.length() - name.length() / 2) + '@' + domain;
  }
}
