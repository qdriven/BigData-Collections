package io.qkits.udf.hive;

import io.qkits.udf.hive.utils.StringUtil;
import org.apache.hadoop.hive.ql.exec.UDF;

public class EncryptLFT extends UDF {

    private final String key = "qDGMdwpREfMHe6aV";

    public String evaluate(String str) {
        try {
            return StringUtil.encrypt(str, key);
        } catch (Exception e) {
            return null;
        }
    }
}
