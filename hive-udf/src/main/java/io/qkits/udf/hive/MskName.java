package io.qkits.udf.hive;

import org.apache.hadoop.hive.ql.exec.UDF;


public class MskName extends UDF {
    public String evaluate(String name) {
        if (name == null || name.isEmpty()) return name;
        StringBuffer sb = new StringBuffer(name.substring(0, 1));
        for (int i=1; i<name.length(); i++) {
            sb.append("*");
        }
        return sb.toString();
    }
}
