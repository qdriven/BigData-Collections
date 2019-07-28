package io.qkits.udf.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

public class EncryptTOC extends UDF {

    public String evaluate(String source) {
        if (source == null || source.length() == 0) {
            return null;
        }
        try {
            return AESCryptor.encrypt(source);
        } catch (Exception e) {
            return null;
        }
    }
}
