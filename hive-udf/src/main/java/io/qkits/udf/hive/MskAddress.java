package io.qkits.udf.hive;

import io.qkits.udf.hive.utils.StringUtil;
import org.apache.hadoop.hive.ql.exec.UDF;

public class MskAddress extends UDF {
    public String evaluate(String address) {
        if (address == null || address.isEmpty()) {
            return address;
        }
        int len = address.length();
        return address.substring(0, len - len/2) + StringUtil.repeat("*", len /2);
    }
}
