package io.qkits.udf.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import io.qkits.udf.hive.utils.crm.SymmetricEncryptionUtil;

public class DecryptHouseCRM extends UDF {

    public String evaluate(String source) {
        if (source == null || source.length() == 0) {
            return null;
        }
        try {
            return SymmetricEncryptionUtil.decryptString(source);
        } catch (Exception e) {
            return null;
        }
    }
}
