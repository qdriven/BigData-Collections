//package io.qkits.udf.hive;
//
//import java.nio.charset.Charset;
//import java.security.Security;
//
//import javax.crypto.Cipher;
//import javax.crypto.spec.SecretKeySpec;
//
//import org.apache.commons.codec.binary.Base64;
//import org.apache.commons.lang.StringUtils;
//import org.slf4j.Logger;
//import org.bouncycastle.jce.provider.BouncyCastleProvider;
//import org.slf4j.LoggerFactory;
//
//public class AESCryptor {
//
//  private static final Logger logger = LoggerFactory.getLogger(AESCryptor.class);
//
//  private static final String ALGORITHM = "AES/ECB/PKCS7Padding";
//
//  private static final String AES = "AES";
//
//  private static final int KEY_BIT_SIZE = 128;
//
//  private static final String KEY = "6A3d8c4E23T";
//
//  static {
//    Security.addProvider(new BouncyCastleProvider());
//  }
//
//  static String encrypt(String target) {
//    try {
//      if (StringUtils.isBlank(target)) {
//        return "";
//      }
//      Cipher cipher = Cipher.getInstance(ALGORITHM);
//      cipher.init(Cipher.ENCRYPT_MODE, initKey(KEY));
//      byte[] encryptResult = cipher.doFinal(target.getBytes(Charset.forName("utf-8")));
//      String
//          unsafeStr =
//          new String(Base64.encodeBase64(encryptResult, false), Charset.forName("utf-8"));
//      return unsafeStr.replace('+', '-').replace('/', '_');
//    } catch (Exception e) {
//      logger.error("AES加密异常，error=" + e.getMessage(), e);
//    }
//    return "";
//  }
//
//  /**
//   * 解密
//   *
//   * @author LXH
//   * @date 2017年9月28日 上午11:11:50
//   */
//  static String decrypt(String target) {
//    try {
//      if (StringUtils.isBlank(target)) {
//        return "";
//      }
//      Cipher cipher = Cipher.getInstance(ALGORITHM);
//      cipher.init(Cipher.DECRYPT_MODE, initKey(KEY));
//      String unsafeStr = target.replace('-', '+').replace('_', '/');
//      byte[]
//          decryptResult =
//          cipher.doFinal(Base64.decodeBase64(unsafeStr.getBytes(Charset.forName("utf-8"))));
//      return new String(decryptResult, Charset.forName("utf-8"));
//    } catch (Exception e) {
//      logger.error("AES解密异常，error=" + e.getMessage(), e);
//    }
//    return "";
//  }
//
//
//  private static SecretKeySpec initKey(String originalKey) {
//    byte[] keys = originalKey.getBytes(Charset.forName("utf-8"));
//    byte[] bytes = new byte[KEY_BIT_SIZE / 8];
//    for (int i = 0; i < bytes.length; i++) {
//      if (keys.length > i) {
//        bytes[i] = keys[i];
//      } else {
//        bytes[i] = 0;
//      }
//    }
//    return new SecretKeySpec(bytes, AES);
//  }
//

//}
