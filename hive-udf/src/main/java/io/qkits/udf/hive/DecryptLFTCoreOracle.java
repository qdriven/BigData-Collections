package io.qkits.udf.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import javax.crypto.Cipher;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import javax.crypto.spec.IvParameterSpec;
import javax.xml.bind.DatatypeConverter;
import java.security.Key;

public class DecryptLFTCoreOracle extends UDF {

    private static final String KEY = "";

    private static final String INIT_VECTOR = "";

    private static final String CIPHER = "DES/CBC/PKCS5Padding";

    private static final String DES = "DES";

    private static final String UTF8 = "UTF-8";

    public String evaluate(String value) {
        if (value == null || value.length() == 0) {
            return null;
        }
        return decrypt(value);
    }

    private static String decrypt(String strCiphertext) {

        byte[] key = KEY.getBytes();
        byte[] ciphertext = DatatypeConverter.parseHexBinary(strCiphertext);

        try {
            DESKeySpec desKeySpec = new DESKeySpec(key);
            SecretKeyFactory factory = SecretKeyFactory.getInstance(DES);
            Key convertSecreKey = factory.generateSecret(desKeySpec);

            IvParameterSpec ivParameterSpec = new IvParameterSpec(INIT_VECTOR.getBytes(UTF8));
            Cipher decryptCipher = Cipher.getInstance(CIPHER);
            decryptCipher.init(Cipher.DECRYPT_MODE, convertSecreKey, ivParameterSpec);
            return new String(decryptCipher.doFinal(ciphertext));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

}
