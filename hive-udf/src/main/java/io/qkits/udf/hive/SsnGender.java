package io.qkits.udf.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import io.qkits.udf.hive.utils.SsnTool;


public class SsnGender extends UDF {
    public String evaluate(String ssn) {
        if (ssn == null) {
            return null;
        } else {
            int ret = SsnTool.getGender(SsnTool.cleanSsn(ssn));
            if (ret == 1) {
                return "M";
            } else if (ret == 0){
                return "F";
            } else {
                return null;
            }
        }
    }
}
