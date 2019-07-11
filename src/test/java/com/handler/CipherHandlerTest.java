package com.handler;

import org.junit.*;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

/**
 * 下文python的aes加解密和测试中的一致
 * <p>
 * #coding: utf8
 * import sys
 * from Crypto.Cipher import AES
 * from binascii import b2a_hex, a2b_hex
 * <p>
 * class prpcrypt():
 * def __init__(self, key, iv):
 * self.key = key
 * self.iv = iv
 * self.mode = AES.MODE_CBC
 * <p>
 * #加密函数，如果text不是16的倍数【加密文本text必须为16的倍数！】，那就补足为16的倍数
 * def encrypt(self, text):
 * cryptor = AES.new(self.key, self.mode, self.iv)
 * #这里密钥key 长度必须为16（AES-128）、24（AES-192）、或32（AES-256）Bytes 长度.目前AES-128足够用
 * length = 16
 * count = len(text)
 * 　　 if(count % length != 0) :
 * add = length - (count % length)
 * 　　 else:
 * add = 0
 * 　　 text = text + ('\0' * add)
 * self.ciphertext = cryptor.encrypt(text)
 * 　　 #因为AES加密时候得到的字符串不一定是ascii字符集的，输出到终端或者保存时候可能存在问题
 * 　　 #所以这里统一把加密后的字符串转化为16进制字符串 ,当然也可以转换为base64加密的内容，可以使用b2a_base64(self.ciphertext)
 * 　　 return b2a_hex(self.ciphertext)
 * <p>
 * #解密后，去掉补足的空格用strip() 去掉
 * def decrypt(self, text):
 * cryptor = AES.new(self.key, self.mode, self.iv)
 * plain_text = cryptor.decrypt(a2b_hex(text))
 * return plain_text.rstrip('\0')
 * <p>
 * if __name__ == '__main__':
 * pc = prpcrypt('keyskeyskeyskeys')      #初始化密钥
 * e = pc.encrypt("0123456789ABCDEF")
 * d = pc.decrypt(e)
 * print e, d
 * e = pc.encrypt("00000000000000000000000000")
 * d = pc.decrypt(e)
 * print e, d
 **/
public class CipherHandlerTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void decryptPWD() throws Exception {
        String src = "b'76208b28cd1384b4f2896d0fe9b7429a'";
        src = src.substring(2, src.length() - 1).toUpperCase();
        src = "8368813c3cbda8dff4cbdb7fd9342fda".toUpperCase();
//        src = "990bf5b1e090c63796ff4758ac93c0b7".toUpperCase();
        String k = "Encrypt@12345678";
        byte[] encrypted1 = IOHandler.hexToByte(src);
        Cipher cipher = Cipher.getInstance("AES/CBC/NoPadding");
        SecretKeySpec keyspec = new SecretKeySpec(k.getBytes(StandardCharsets.UTF_8), "AES");
        IvParameterSpec ivspec = new IvParameterSpec(k.getBytes(StandardCharsets.UTF_8));
        cipher.init(Cipher.DECRYPT_MODE, keyspec, ivspec);
        byte[] original = cipher.doFinal(encrypted1);
        String originalString = new String(original);
        System.out.println(originalString.trim());
    }

    @Test
    public void encryptPWD() throws Exception {
        String src = "cii2018";
        String k = "Encrypt@12345678";
        Cipher cipher = Cipher.getInstance("AES/CBC/NoPadding");
        int blockSize = cipher.getBlockSize();
        byte[] dataBytes = src.getBytes(StandardCharsets.UTF_8);
        int plaintextLength = dataBytes.length;
        if (plaintextLength % blockSize != 0) {
            plaintextLength = plaintextLength + (blockSize - (plaintextLength % blockSize));
        }
        byte[] plaintext = new byte[plaintextLength];
        System.arraycopy(dataBytes, 0, plaintext, 0, dataBytes.length);

        SecretKeySpec keyspec = new SecretKeySpec(k.getBytes(StandardCharsets.UTF_8), "AES");
        IvParameterSpec ivspec = new IvParameterSpec(k.getBytes(StandardCharsets.UTF_8));
        cipher.init(Cipher.ENCRYPT_MODE, keyspec, ivspec);
        byte[] encrypted = cipher.doFinal(plaintext);
        String EncStr = IOHandler.byteToHex(encrypted);
        System.out.println(EncStr.toLowerCase());
    }
}