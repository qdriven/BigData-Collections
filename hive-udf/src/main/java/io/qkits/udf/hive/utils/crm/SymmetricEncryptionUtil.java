package io.qkits.udf.hive.utils.crm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SymmetricEncryptionUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(SymmetricEncryptionUtil.class);
  //密码
  private static final String KEY = "nah!iz2gnehc0";
  //加密算法
  private static final String ALGORITHM = EncryptionConstants.SYMMETRIC_ALGORITHM_DES;
  //加密模式
  private static final String MODE = EncryptionConstants.ALGORITHM_MODE_CFB;
  //填充模式
  private static final String PADDING = EncryptionConstants.ALGORITHM_PADDING_NOPADDING;
  //iv向量
  private static final String IVPARAM = "naw!02ul";
  //密码长度
  private static final int KEYSIZE = EncryptionConstants.ALGORITHM_KEY_BIT_SIZE_64;


  public static String encryptString(String source) {
    try {
      if (hasText(source)) {
        source = new String(source.getBytes(), EncryptionConstants.DEFAULTCHARSET);
//                source = SymmetricEncryption.encrypt(source,
//                        KEY, ALGORITHM, MODE, PADDING, IVPARAM, KEYSIZE);
      }
    } catch (Exception e) {
      LOGGER.error("调用三方工具加密异常", e);
    }
    return source;
  }


  public static String decryptString(String source) {
    try {
      if (hasText(source)) {
//                source = SymmetricEncryption.decrypt(source,
//                        KEY, ALGORITHM, MODE, PADDING, IVPARAM, KEYSIZE);
      }
    } catch (Exception e) {
      LOGGER.error("调用三方工具解密异常", e);
    }
    return source;
  }

  public static boolean hasText(String str) {
    return (str != null && !str.isEmpty() && containsText(str));
  }

  private static boolean containsText(CharSequence str) {
    int strLen = str.length();
    for (int i = 0; i < strLen; i++) {
      if (!Character.isWhitespace(str.charAt(i))) {
        return true;
      }
    }
    return false;
  }
}

