package io.qkits.udf.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import cn.hutool.crypto.symmetric.AES;

public class DecryptCredit extends UDF {

  private static final String AES_KEY = "Yna#DSseD+S2W-A_";

  public String evaluate(String source) {
    if (source == null || source.length() == 0) {
      return null;
    }
    try {
      return new AES(AES_KEY.getBytes()).decryptStr(source);
    } catch (Exception e) {
      return null;
    }
  }
}
