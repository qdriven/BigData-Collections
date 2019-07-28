package io.qkits.udf.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import io.qkits.udf.hive.utils.StringUtil;

public class EncryptLabel extends UDF {

    private final String key = "MNpvDcqJEQv20lv8";

    public String evaluate(String str) {
        try {
            return StringUtil.encrypt(str, key);
        } catch (Exception e) {
            return null;
        }
    }
}
