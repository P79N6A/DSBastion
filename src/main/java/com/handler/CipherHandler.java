package com.handler;

import com.bean.MaskBean;
import com.util.HexBin;

import javax.crypto.*;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.*;

public class CipherHandler {

    private static byte[] aes(byte[] content, String en_key, int mode) throws Exception {
        if (Cipher.DECRYPT_MODE == mode) content = HexBin.decode(new String(content, StandardCharsets.UTF_8));
        KeyGenerator kgen = KeyGenerator.getInstance("AES");
        SecureRandom secureRandom = SecureRandom.getInstance("SHA1PRNG");
        secureRandom.setSeed(en_key.getBytes(StandardCharsets.UTF_8));
        kgen.init(128, secureRandom);
        SecretKey secretKey = kgen.generateKey();
        SecretKeySpec key = new SecretKeySpec(secretKey.getEncoded(), "AES");
        Cipher cipher = Cipher.getInstance("AES");
        cipher.init(mode, key);
        byte[] bytes = cipher.doFinal(content);
        if (Cipher.ENCRYPT_MODE == mode) bytes = HexBin.encode(bytes).getBytes(StandardCharsets.UTF_8);
        return bytes;
    }

    public static byte[] decrypt(byte[] bytes, MaskBean maskBean) throws Exception {
        if (maskBean == null) return bytes;
        int type = maskBean.getType();
        if (2 != type) return bytes;
        String name = maskBean.getName();
        String valueType = maskBean.getValueType();
        String param = maskBean.getParam();
        if (valueType.equalsIgnoreCase("STRING")) {
            if ("STRING_ENCRY_AES".equalsIgnoreCase(name))
                return aes(bytes, param, Cipher.DECRYPT_MODE);
        }
        return bytes;
    }

    public static byte[] encrypt(byte[] bytes, MaskBean maskBean) throws Exception {
        if (maskBean == null) return bytes;
        int type = maskBean.getType();
        if (2 != type) return bytes;
        String name = maskBean.getName();
        String valueType = maskBean.getValueType();
        String param = maskBean.getParam();
        if (valueType.equalsIgnoreCase("STRING")) {
            if ("STRING_ENCRY_AES".equalsIgnoreCase(name))
                return aes(bytes, param, Cipher.ENCRYPT_MODE);
        }
        return bytes;
    }
}
