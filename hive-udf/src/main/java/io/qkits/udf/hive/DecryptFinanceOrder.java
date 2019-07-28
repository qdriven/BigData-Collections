package io.qkits.udf.hive;

import com.bkjk.cf.platform.encryption.utils.AesCrypt;
import org.apache.hadoop.hive.ql.exec.UDF;

public class DecryptFinanceOrder extends UDF {

    public String evaluate(String source) {
        if (source == null || source.length() == 0) {
            return null;
        }
        try {
            return AesCrypt.aesBkjkDecrypt(source);
        } catch (Exception e) {
            return null;
        }
    }

}
