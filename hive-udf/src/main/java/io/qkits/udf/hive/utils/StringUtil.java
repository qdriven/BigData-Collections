package io.qkits.udf.hive.utils;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;

import java.security.SecureRandom;

import cn.hutool.core.codec.Base64Decoder;
import cn.hutool.core.codec.Base64Encoder;


public class StringUtil {

  public static String repeat(String seed, int num) {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < num; i++) {
      sb.append(seed);
    }
    return sb.toString();
  }

  public static String encrypt(String source, String key) throws Exception {
    if (source == null || source.length() == 0) {
      return source;
    }
    SecureRandom random = new SecureRandom();
    DESKeySpec keySpec = new DESKeySpec(key.getBytes());
    SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("des");
    SecretKey secretKey = keyFactory.generateSecret(keySpec);

    Cipher cipher = Cipher.getInstance("des");
    cipher.init(Cipher.ENCRYPT_MODE, secretKey, random);
    byte[] cipherData = cipher.doFinal(source.getBytes());
    return Base64Encoder.encode(cipherData);
  }

  public static String decrypt(String str, String key) {
    if (str == null || str.length() == 0) {
      return str;
    }
    try {
      SecureRandom random = new SecureRandom();
      DESKeySpec keySpec = new DESKeySpec(key.getBytes());
      SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("des");
      SecretKey secretKey = keyFactory.generateSecret(keySpec);

      Cipher cipher = Cipher.getInstance("des");
      cipher.init(Cipher.DECRYPT_MODE, secretKey, random);
      byte[] cipherData = cipher.doFinal(Base64Decoder.decode(str));
      return new String(cipherData);
    } catch (Exception e) {
      return null;
    }
  }
}
