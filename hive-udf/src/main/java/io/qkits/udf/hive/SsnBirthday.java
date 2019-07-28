package io.qkits.udf.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import io.qkits.udf.hive.utils.SsnTool;

public class SsnBirthday extends UDF {
    public String evaluate(String ssn) {
        if (ssn == null) {
            return null;
        } else {
            String ret = SsnTool.getBirthday(SsnTool.cleanSsn(ssn));
            if (ret.isEmpty()) {
                return null;
            } else {
                return ret;
            }
        }
    }
}
