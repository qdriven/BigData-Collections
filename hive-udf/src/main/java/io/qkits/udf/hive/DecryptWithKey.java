package io.qkits.udf.hive;

import io.qkits.udf.hive.utils.StringUtil;
import org.apache.hadoop.hive.ql.exec.UDF;

public class DecryptWithKey extends UDF {
    public String evaluate(String str, String key) {
        try {
            return StringUtil.decrypt(str, key);
        } catch (Exception e) {
            return null;
        }
    }
}
