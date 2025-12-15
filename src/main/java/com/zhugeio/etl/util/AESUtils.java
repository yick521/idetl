package com.zhugeio.etl.util;

import javax.crypto.*;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * Created by hanqi on 18-2-28.
 */
public class AESUtils {
    /* http://stackoverflow.org.cn/front/ask/view?ask_id=48246 */
    public static final String KEY_ALGORITHM = "AES";
    public static final String KEY_STR = "kmI/3v+FOGDNqwrsZzrAXg==";
    public static SecretKey key = genKey();

    public static SecretKey genKey(){
        try{
            /*//1.构造密钥生成器，指定为AES算法,不区分大小写
            KeyGenerator keygen=KeyGenerator.getInstance("AES");
            //2.根据ecnodeRules规则初始化密钥生成器
            //生成一个128位的随机源,根据传入的字节数组
            keygen.init(128);
            //3.产生原始对称密钥
            SecretKey original_key=keygen.generateKey();

            //4.获得原始对称密钥的字节数组
            byte [] raw=original_key.getEncoded();

            String encodedKey = Base64.getEncoder().encodeToString(raw);
            System.out.println("key:"+encodedKey+":");*/
            byte[] decodedKey = Base64.getDecoder().decode(KEY_STR);

            //5.根据字节数组生成AES密钥
            SecretKey key=new SecretKeySpec(decodedKey, KEY_ALGORITHM);

            return key;
        }catch (Exception e){
            throw new RuntimeException("获取AES密钥失败:"+e);
        }
    }

    //加密
    public static String aesEncode(String content){
        try {
            Cipher cipher=Cipher.getInstance(KEY_ALGORITHM);
            //7.初始化密码器，第一个参数为加密(Encrypt_mode)或者解密解密(Decrypt_mode)操作，第二个参数为使用的KEY
            cipher.init(Cipher.ENCRYPT_MODE, key);
            //8.获取加密内容的字节数组(这里要设置为utf-8)不然内容中如果有中文和英文混合中文就会解密为乱码
            byte [] byteEncode=content.getBytes("utf-8");
            //9.根据密码器的初始化方式--加密：将数据加密
            byte [] byteAES=cipher.doFinal(byteEncode);
            //10.将加密后的数据转换为字符串
            // 使用Java 8+标准Base64替代sun.misc.BASE64Encoder
            String aesEncodedStr = Base64.getEncoder().encodeToString(byteAES);
            //11.将字符串返回
            return aesEncodedStr;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (NoSuchPaddingException e) {
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        } catch (IllegalBlockSizeException e) {
            e.printStackTrace();
        } catch (BadPaddingException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return null;
    }

    //解密
    public static String aesDncode(String content){
        try {
            Cipher cipher=Cipher.getInstance("AES");
            //7.初始化密码器，第一个参数为加密(Encrypt_mode)或者解密(Decrypt_mode)操作，第二个参数为使用的KEY
            cipher.init(Cipher.DECRYPT_MODE, key);
            //8.将加密并编码后的内容解码成字节数组
            // 使用Java 8+标准Base64替代sun.misc.BASE64Decoder
            // 先清理换行符，然后解码
            String cleanContent = content.replaceAll("\\s", "");
            byte [] byteContent = Base64.getDecoder().decode(cleanContent);
            /*
             * 解密
             */
            byte [] byteDecode=cipher.doFinal(byteContent);
            String aesDecodedStr=new String(byteDecode,"utf-8");
            return aesDecodedStr;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (NoSuchPaddingException e) {
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        } catch (IllegalBlockSizeException e) {
            e.printStackTrace();
        } catch (BadPaddingException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

        return null;
    }

    public static void main(String[] args) {
        AESUtils se=new AESUtils();
        String content = "fuliao|2018-10-11";
        String miwen2 = se.aesEncode(content);
        String miwen1 = "+a6LsqlT14k39Q7Of1yWvZkYLIUMw1WVs2MVklMNLFhcEUCsyJo/75gwiK15SWR7SUc5KKi6oJIh\nkmpusw6UuW8haaerVWVPe1esi+j6uIqgUtsho8nSvp/Z3e10vG5DCsKhXnbddHXl+M7aCFzImvyc\n47N27jmQtNCCYjNAkS/qJggOgVQNTTJ//NnUElusBzL/8sST7PmkdGHbmXbSCS+SNR90QE2l+LLN\npu2BxzY=";
        System.out.println("a:"+miwen1+":a");
        System.out.println(se.aesDncode(miwen2));
    }
}
