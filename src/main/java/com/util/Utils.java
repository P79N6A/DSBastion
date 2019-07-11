/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Utils {

    private final static int DEFAULT_BUFFER_SIZE = 1024 * 4;
    private static Set<String> mysqlKeyWords;
    private static Set<String> oracleKeyWords;
    private static Set<String> pgKeyWords;

    public static String read(InputStream in) {
        InputStreamReader reader;
        reader = new InputStreamReader(in, StandardCharsets.UTF_8);
        return read(reader);
    }


    public static long copy(InputStream input, OutputStream output) throws IOException {
        final int EOF = -1;

        byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];

        long count = 0;
        int n = 0;
        while (EOF != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
            count += n;
        }
        return count;
    }

    public static String read(Reader reader) {
        try {

            StringWriter writer = new StringWriter();

            char[] buffer = new char[DEFAULT_BUFFER_SIZE];
            int n = 0;
            while (-1 != (n = reader.read(buffer))) {
                writer.write(buffer, 0, n);
            }

            return writer.toString();
        } catch (IOException ex) {
            throw new IllegalStateException("read error", ex);
        }
    }

    public static String read(Reader reader, int length) {
        try {
            char[] buffer = new char[length];

            int offset = 0;
            int rest = length;
            int len;
            while ((len = reader.read(buffer, offset, rest)) != -1) {
                rest -= len;
                offset += len;

                if (rest == 0) {
                    break;
                }
            }

            return new String(buffer, 0, length - rest);
        } catch (IOException ex) {
            throw new IllegalStateException("read error", ex);
        }
    }

    public static String toString(Date date) {
        if (date == null) {
            return null;
        }
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return format.format(date);
    }

    public static String toString(StackTraceElement[] stackTrace) {
        StringBuilder buf = new StringBuilder();
        for (StackTraceElement item : stackTrace) {
            buf.append(item.toString());
            buf.append("\n");
        }
        return buf.toString();
    }


    private static byte[] md5Bytes(String text) {
        MessageDigest msgDigest = null;
        try {
            msgDigest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("System doesn't support MD5 algorithm.");
        }
        msgDigest.update(text.getBytes());
        return msgDigest.digest();
    }

    public static String md5(String text) {
        byte[] bytes = md5Bytes(text);
        return HexBin.encode(bytes, false);
    }

    public static void putLong(byte[] b, int off, long val) {
        b[off + 7] = (byte) (val >>> 0);
        b[off + 6] = (byte) (val >>> 8);
        b[off + 5] = (byte) (val >>> 16);
        b[off + 4] = (byte) (val >>> 24);
        b[off + 3] = (byte) (val >>> 32);
        b[off + 2] = (byte) (val >>> 40);
        b[off + 1] = (byte) (val >>> 48);
        b[off + 0] = (byte) (val >>> 56);
    }

    public static boolean equals(Object a, Object b) {
        return Objects.equals(a, b);
    }


    public static String hex_t(long hash) {
        byte[] bytes = new byte[8];

        bytes[7] = (byte) (hash);
        bytes[6] = (byte) (hash >>> 8);
        bytes[5] = (byte) (hash >>> 16);
        bytes[4] = (byte) (hash >>> 24);
        bytes[3] = (byte) (hash >>> 32);
        bytes[2] = (byte) (hash >>> 40);
        bytes[1] = (byte) (hash >>> 48);
        bytes[0] = (byte) (hash >>> 56);

        char[] chars = new char[18];
        chars[0] = 'T';
        chars[1] = '_';
        for (int i = 0; i < 8; ++i) {
            byte b = bytes[i];

            int a = b & 0xFF;
            int b0 = a >> 4;
            int b1 = a & 0xf;

            chars[i * 2 + 2] = (char) (b0 + (b0 < 10 ? 48 : 55));
            chars[i * 2 + 3] = (char) (b1 + (b1 < 10 ? 48 : 55));
        }

        return new String(chars);
    }

    public static long fnv_64(String input) {
        return FnvHash.fnv1a_64(input);
    }

    public static long fnv_64_lower(String key) {
        return FnvHash.fnv1a_64_lower(key);
    }

    public static long fnv_32_lower(String key) {
        return FnvHash.fnv_32_lower(key);
    }

    private static void loadFromFile(String path, Set<String> set) {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
             BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            for (; ; ) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }

                line = line.trim().toLowerCase();

                if (line.length() == 0) {
                    continue;
                }
                set.add(line);
            }
        } catch (Exception ex) {
            // skip
        }
    }

    public static boolean isMysqlKeyword(String name) {
        if (name == null) {
            return false;
        }

        String name_lower = name.toLowerCase();

        Set<String> words = mysqlKeyWords;

        if (words == null) {
            words = new HashSet<String>();
            loadFromFile("jdbc/parser/mysql/keywords", words);
            mysqlKeyWords = words;
        }

        return words.contains(name_lower);
    }

    public static boolean isOracleKeyword(String name) {
        if (name == null) {
            return false;
        }

        String name_lower = name.toLowerCase();

        Set<String> words = oracleKeyWords;

        if (words == null) {
            words = new HashSet<String>();
            loadFromFile("jdbc/parser/oracle/keywords", words);
            oracleKeyWords = words;
        }

        return words.contains(name_lower);
    }

    public static boolean isPGKeyword(String name) {
        if (name == null) {
            return false;
        }

        String name_lower = name.toLowerCase();

        Set<String> words = pgKeyWords;

        if (words == null) {
            words = new HashSet<String>();
            loadFromFile("jdbc/parser/postgresql/keywords", words);
            pgKeyWords = words;
        }

        return words.contains(name_lower);
    }
}
