package io.qkits.udf.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

public class Hex2Dec extends UDF {
    public String evaluate(String hex) {
        if (hex == null || hex.isEmpty()) {
            return hex;
        }
        return String.valueOf(Integer.parseInt(hex,16));
    }
}
