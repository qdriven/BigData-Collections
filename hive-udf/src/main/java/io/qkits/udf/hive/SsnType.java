package io.qkits.udf.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import io.qkits.udf.hive.utils.SsnTool;


public class SsnType extends UDF {
    public Integer evaluate(String ssn) {
        if (ssn == null) {
            return -1;
        } else {
            String ssn_clean = SsnTool.cleanSsn(ssn);
            String ret;

            ret = SsnTool.isValidSSN(ssn_clean);
            if (! ret.isEmpty()) {
                return 1;
            }
            ret = SsnTool.isValidSSNHK(ssn_clean.replace('（', '(').replace('）', ')'));
            if (! ret.isEmpty()) {
                return 2;
            }

            ret = SsnTool.isValidSSNTW(ssn_clean);
            if (! ret.isEmpty()) {
                return 3;
            }

            return -1;
        }
    }
}