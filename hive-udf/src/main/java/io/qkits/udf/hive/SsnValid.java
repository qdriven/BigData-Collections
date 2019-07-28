package io.qkits.udf.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import io.qkits.udf.hive.utils.SsnTool;
import io.qkits.udf.hive.utils.StringUtil;
public class SsnValid extends UDF{
    public String evaluate(String ssn) {
        if (ssn == null) {
            return null;
        } else {
            String ssn_clean = SsnTool.cleanSsn(ssn);
            String ret;
            ret = SsnTool.isValidSSN(ssn_clean);
            if (! ret.isEmpty()) {
                return ret;
            }
            ret = SsnTool.isValidSSNHK(ssn_clean.replace('（', '(').replace('）', ')'));
            if (! ret.isEmpty()) {
                return ret;
            }

            ret = SsnTool.isValidSSNTW(ssn_clean);
            if (! ret.isEmpty()) {
                return ret;
            }

            return null;
        }
    }
}
