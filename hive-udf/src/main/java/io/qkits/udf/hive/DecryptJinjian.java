package io.qkits.udf.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import io.qkits.udf.hive.utils.StringUtil;

public class DecryptJinjian extends UDF {

    private static final String KEY = "BuLj7N6SGKnoCoMp";

    public String evaluate(String str) {
        try {
            return StringUtil.decrypt(str, KEY);
        } catch (Exception e) {
            return null;
        }
    }

}
