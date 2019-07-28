package io.qkits.udf.hive.utils.crm;

public class EncryptionConstants {

    public static final String DEFAULTCHARSET = "utf-8";

    public static final String ASYMMETRIC_ALGORITHM_DH = "DH";
    public static final String ASYMMETRIC_ALGORITHM_RSA = "RSA";
    public static final String ASYMMETRIC_ALGORITHM_ELGAMAL = "ElGamal";
    public static final String ASYMMETRIC_ALGORITHM_ECC = "ECC";

    public static final String SYMMETRIC_ALGORITHM_AES = "AES";
    public static final String SYMMETRIC_ALGORITHM_DES = "DES";
    public static final String SYMMETRIC_ALGORITHM_BLOWFISH = "Blowfish";
    public static final String SYMMETRIC_ALGORITHM_DE = "DESede";
    public static final String SYMMETRIC_ALGORITHM_CTR = "CTR";
    public static final String SYMMETRIC_ALGORITHM_RIJNDAEL = "Rijndael";
    public static final String SYMMETRIC_ALGORITHM_RC4 = "RC4";

    public static final String ALGORITHM_MODE_ECB = "ECB";
    public static final String ALGORITHM_MODE_CBC = "CBC";
    public static final String ALGORITHM_MODE_CFB = "CFB";
    public static final String ALGORITHM_MODE_OFB = "OFB";
    public static final String ALGORITHM_MODE_PCBC = "PCBC";
    public static final String ALGORITHM_MODE_CTR = "CTR";

    public static final int ALGORITHM_KEY_BIT_SIZE_64 = 64;
    public static final int ALGORITHM_KEY_BIT_SIZE_128 = 128;
    public static final int ALGORITHM_KEY_BIT_SIZE_192 = 192;
    public static final int ALGORITHM_KEY_BIT_SIZE_256 = 256;
    public static final int ALGORITHM_KEY_BIT_SIZE_1024 = 1024;

    public static final String ALGORITHM_PADDING_NOPADDING = "NoPadding";
    public static final String ALGORITHM_PADDING_PKCS5PADDING = "PKCS5Padding";
    public static final String ALGORITHM_PADDING_ISO10126PADDING = "ISO10126Padding";
    public static final String ALGORITHM_PADDING_PKCS7PADDING = "PKCS7Padding";
    public static final String ALGORITHM_PADDING_SSL3PADDING = "SSL3Padding";
    public static final String ALGORITHM_PADDING_NONE = "NONE";


    public EncryptionConstants() {
    }
}
