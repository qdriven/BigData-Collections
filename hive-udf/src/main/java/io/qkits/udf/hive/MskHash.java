package io.qkits.udf.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.commons.codec.digest.DigestUtils;

public class MskHash extends UDF {
    private static String salt = "BkJKsh@0!*";

    public String evaluate(String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }
        return DigestUtils.sha256Hex(str + salt);
    }
}
