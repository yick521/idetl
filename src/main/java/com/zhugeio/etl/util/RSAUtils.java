package com.zhugeio.etl.util;


import javax.crypto.Cipher;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by hanqi on 18-2-28.
 */
public class RSAUtils {

    public static final String KEY_ALGORITHM = "RSA";
    private static final String PUBLIC_KEY = "RSAPublicKey";
    private static final String PRIVATE_KEY = "RSAPrivateKey";

    //获得公钥
    public static String getPublicKey(Map<String, Object> keyMap) throws Exception {
        //获得map中的公钥对象 转为key对象
        Key key = (Key) keyMap.get(PUBLIC_KEY);
        //byte[] publicKey = key.getEncoded();
        //编码返回字符串
        return encryptBASE64(key.getEncoded());
    }

    //获得私钥
    public static String getPrivateKey(Map<String, Object> keyMap) throws Exception {
        //获得map中的私钥对象 转为key对象
        Key key = (Key) keyMap.get(PRIVATE_KEY);
        //byte[] privateKey = key.getEncoded();
        //编码返回字符串
        return encryptBASE64(key.getEncoded());
    }

    //解码返回byte
    public static byte[] decryptBASE64(String key) throws Exception {
        // 使用Java 8+标准Base64替代sun.misc.BASE64Decoder
        // 清理换行符以确保兼容性
        String cleanKey = key.replaceAll("\\s", "");
        return Base64.getDecoder().decode(cleanKey);
    }

    //编码返回字符串
    public static String encryptBASE64(byte[] key) throws Exception {
        // 使用Java 8+标准Base64替代sun.misc.BASE64Encoder
        return Base64.getEncoder().encodeToString(key);
    }

    //map对象中存放公私钥
    public static Map<String, Object> initKey() throws Exception {
        //获得对象 KeyPairGenerator 参数 RSA 1024个字节
        KeyPairGenerator keyPairGen = KeyPairGenerator.getInstance(KEY_ALGORITHM);
        keyPairGen.initialize(1024);
        //通过对象 KeyPairGenerator 获取对象KeyPair
        KeyPair keyPair = keyPairGen.generateKeyPair();

        //通过对象 KeyPair 获取RSA公私钥对象RSAPublicKey RSAPrivateKey
        RSAPublicKey publicKey = (RSAPublicKey) keyPair.getPublic();
        RSAPrivateKey privateKey = (RSAPrivateKey) keyPair.getPrivate();
        //公私钥对象存入map中
        Map<String, Object> keyMap = new HashMap<String, Object>(2);
        keyMap.put(PUBLIC_KEY, publicKey);
        keyMap.put(PRIVATE_KEY, privateKey);
        return keyMap;
    }

    public static PublicKey str2PK(String pkStr) throws Exception {
        PublicKey publicKey = KeyFactory.getInstance("RSA").generatePublic(new X509EncodedKeySpec(decryptBASE64(pkStr)));
        return publicKey;
    }

    public static PrivateKey str2PriK(String pkStr) throws Exception {
        PrivateKey privateKey = KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(decryptBASE64(pkStr)));
        return privateKey;
    }

    //利用RSA私钥加密明文content
    public static String encryptWithRSA(String content, PrivateKey privateKey){
        try{
            Cipher cipher = Cipher.getInstance("RSA");
            //设置为加密模式,并将私钥给cipher。
            cipher.init(Cipher.ENCRYPT_MODE, privateKey);
            //获得密文
            byte[] secret = cipher.doFinal(content.getBytes());

            // 使用Java 8+标准Base64替代sun.misc.BASE64Encoder
            return Base64.getEncoder().encodeToString(secret);

        }catch (Exception e){
            throw new RuntimeException("加密:"+content+":失败");
        }
    }

    //利用RSA公钥解密密文1到明文
    public static String decryptWithRSA(String secret, PublicKey publicKey){
        try{
            Cipher cipher = Cipher.getInstance("RSA");
            //设置为加密模式,并将私钥给cipher。
            cipher.init(Cipher.DECRYPT_MODE, publicKey);
            //获得密文
            // 使用Java 8+标准Base64替代sun.misc.BASE64Decoder
            // 清理换行符以确保兼容性
            String cleanSecret = secret.replaceAll("\\s", "");
            byte[] content = cipher.doFinal(Base64.getDecoder().decode(cleanSecret));

            return new String(content, StandardCharsets.UTF_8);

        }catch (Exception e){
            throw new RuntimeException("解密:"+secret+":失败");
        }
    }
}
