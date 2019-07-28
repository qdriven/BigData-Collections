package io.qkits.udf.hive;

import org.apache.hadoop.hive.ql.exec.UDF;



public class StrClear extends UDF {
    static String[] specials = {"\n", "\r", "\u0001", "\u0002"};

    public String evaluate(String str) {
        if (str == null) {
            return null;
        }
        for(String spec: specials) {
            str = str.replace(spec, "");
        }
        return str;
    }
}
