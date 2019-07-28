package io.qkits.udf.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

public class DecryptTOC extends UDF {

    public String evaluate(String source) {
        if (source == null || source.length() == 0) {
            return null;
        }
        try {
            return AESCryptor.decrypt(source);
        } catch (Exception e) {
            return null;
        }
    }
}
