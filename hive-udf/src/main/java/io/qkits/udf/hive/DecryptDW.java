package io.qkits.udf.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import io.qkits.udf.hive.utils.StringUtil;

public class DecryptDW extends UDF {

    public String evaluate(String str) {
        try {
            return StringUtil.decrypt(str, Encrypt.KEY);
        } catch (Exception e) {
            return null;
        }
    }

}
