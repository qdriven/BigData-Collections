package io.qkits.udf.hive;

import io.qkits.udf.hive.utils.StringUtil;
import org.apache.hadoop.hive.ql.exec.UDF;


public class Encrypt extends UDF {

    protected static final String KEY = "BkJK@0!*fAfjNc6qe6yHlJlU";

    public String evaluate(String str) {
        try {
            return StringUtil.encrypt(str, KEY);
        } catch (Exception e) {
            return null;
        }
    }
}
