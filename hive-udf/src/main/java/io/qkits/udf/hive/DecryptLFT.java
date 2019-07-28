package io.qkits.udf.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;

public class DecryptLFT extends UDF {
    private static final String CIPHER = "DES/CBC/PKCS5Padding";


    public String evaluate(String strKey, String init_vector, String strCiphertext) {
        if (strCiphertext == null || strCiphertext.length() == 0) {
            return null;
        }
        try {
            byte[] key = strKey.getBytes();
            byte[] ciphertext = DatatypeConverter.parseHexBinary(strCiphertext);

            SecretKeySpec secretKeySpec = new SecretKeySpec(key, "DES");
            IvParameterSpec ivParameterSpec = new IvParameterSpec(init_vector.getBytes("UTF-8"));
            Cipher decryptCipher = Cipher.getInstance(CIPHER);
            decryptCipher.init(Cipher.DECRYPT_MODE, secretKeySpec,ivParameterSpec);

            return new String(decryptCipher.doFinal(ciphertext));
        } catch (Exception e) {
            return null;
        }
    }
}
