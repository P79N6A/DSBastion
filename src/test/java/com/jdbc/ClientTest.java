package com.jdbc;

import io.netty.channel.RecvByteBufAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

/**
 * The {@link RecvByteBufAllocator} that automatically increases and
 * decreases the predicted buffer size on feed back.
 * <p>
 * It gradually increases the expected number of readable bytes if the previous
 * read fully filled the allocated buffer.  It gradually decreases the expected
 * number of readable bytes if the read operation was not able to fill a certain
 * amount of the allocated buffer two times consecutively.  Otherwise, it keeps
 * returning the same prediction.
 * <p>
 * 所以这里客户端在内容不足1024的时候填充到1024
 */

public class ClientTest {

    private Socket socket = null;

    @Before
    public void setUp() throws Exception {
        socket = new Socket("127.0.0.1", 8007);
    }

    @After
    public void tearDown() throws Exception {
        if (socket != null) socket.close();
    }

    private byte[] filling(ByteBuffer buffer) {
        byte[] bytes = new byte[1024];
        int wid = buffer.position();
        System.arraycopy(buffer.array(), 0, bytes, 0, wid);
        if (wid < 1024) for (int i = wid; i < 1024; i++) {
            bytes[i] = (byte) ~0;
        }
        return bytes;
    }

//    @Test
//    public void testFilling() {
//        ByteBuffer buffer = ByteBuffer.allocate(4);
//        buffer.putInt(2);
//        byte[] bytes = filling(buffer);
//        assertEquals(2, bytes2Int(bytes));
//        assertEquals(1024, bytes.length);
//    }

    /**
     * jdbc:mysql2://host:port/dbname?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai&zeroDateTimeBehavior=CONVERT_TO_NULL
     */
    @Test
    public void testConnect() throws IOException {
        byte[] ak = "3074559825718491745".getBytes(StandardCharsets.UTF_8);
        byte[] mac = "14:fe:b5:e7:0b:a9".getBytes(StandardCharsets.UTF_8);
        DataInputStream in = new DataInputStream(socket.getInputStream());
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.put((byte) 0x02);
        buffer.putShort((short) ak.length);
        buffer.put(ak);
        buffer.putShort((short) mac.length);
        buffer.put(mac);
        byte[] bytes = filling(buffer);
        out.write(bytes);
        out.flush();
        buffer.clear();
        short result = in.readByte();
        if (result == 0) System.out.println("connect success");
        else System.out.println("create connect failure: " + readShortString(in));
    }


    @Test
    public void testPrepareStatement() throws IOException {
        testConnect();
        DataInputStream in = new DataInputStream(socket.getInputStream());
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());

        byte[] sql = "select * from lgservice where service_id=? and service_name=?"
                .getBytes(StandardCharsets.UTF_8);

        byte[] user = "test".getBytes(StandardCharsets.UTF_8);
        ByteBuffer tempBuffer = ByteBuffer.allocate(1024);
        byte[] methodName, outBytes;
        short result;

        methodName = "prepareStatement".getBytes(StandardCharsets.UTF_8);
        tempBuffer.put((byte) 0x03);
        tempBuffer.put((byte) methodName.length);
        tempBuffer.put(methodName);
        tempBuffer.putShort((short) 0);
//        tempBuffer.putShort((short) user.length);
//        tempBuffer.put(user);
        tempBuffer.put((byte) 1);
        tempBuffer.putInt(sql.length);
        tempBuffer.put(sql);
        outBytes = filling(tempBuffer);
        out.write(outBytes);
        out.flush();
        tempBuffer.clear();
        result = in.readByte();
        String stmtId = null;
        if (result == 0) {
            stmtId = readShortString(in);
            System.out.println("prepareStatement success,stmt[" + stmtId + "]");
        } else System.out.println("create statement failure: " + readShortString(in));

        if (stmtId != null) {
            byte[] stmt = stmtId.getBytes(StandardCharsets.UTF_8);

            methodName = "setInt".getBytes(StandardCharsets.UTF_8);
            tempBuffer.put((byte) 0x07);
            tempBuffer.putShort((short) stmt.length);
            tempBuffer.put(stmt);
            tempBuffer.put((byte) methodName.length);
            tempBuffer.put(methodName);
            tempBuffer.putInt(1);
            tempBuffer.putInt(100);
            outBytes = filling(tempBuffer);
            out.write(outBytes);
            out.flush();
            tempBuffer.clear();
            result = in.readByte();
            if (result == 0) System.out.println("setInt success");
            else System.out.println("setInt failure");

            methodName = "setString".getBytes(StandardCharsets.UTF_8);
            byte[] value = "test".getBytes(StandardCharsets.UTF_8);
            tempBuffer.put((byte) 0x07);
            tempBuffer.putShort((short) stmt.length);
            tempBuffer.put(stmt);
            tempBuffer.put((byte) methodName.length);
            tempBuffer.put(methodName);
            tempBuffer.putInt(2);
            tempBuffer.putInt(value.length);
            tempBuffer.put(value);
            outBytes = filling(tempBuffer);
            out.write(outBytes);
            out.flush();
            tempBuffer.clear();
            result = in.readByte();
            if (result == 0) System.out.println("setString success");
            else System.out.println("setString failure");

            methodName = "executeQuery".getBytes(StandardCharsets.UTF_8);
            tempBuffer.put((byte) 0x07);
            tempBuffer.putShort((short) stmt.length);
            tempBuffer.put(stmt);
            tempBuffer.put((byte) methodName.length);
            tempBuffer.put(methodName);
            outBytes = filling(tempBuffer);
            out.write(outBytes);
            out.flush();
            tempBuffer.clear();
            result = in.readByte();
            if (result == 0) {
                System.out.println("executeQuery success");
                String rsId = readShortString(in);
                assert rsId != null;
                System.out.println("resultSet: " + rsId);
                System.out.println("**********RSMeta**********");
                short colCount = in.readShort();
                for (int i = 0; i < colCount; i++) {
                    System.out.print("catalogName:[" + readShortString(in));
                    System.out.print("],schemaName:[" + readShortString(in));
                    System.out.print("],tableName:[" + readShortString(in));
                    System.out.print("],columnLabel:[" + readShortString(in));
                    System.out.print("],columnName:[" + readShortString(in));
                    System.out.print("],columnTypeName:[" + readShortString(in));
                    System.out.print("],columnDisplaySize:[" + in.readInt());
                    System.out.print("],precision:[" + in.readInt());
                    System.out.print("],scale:[" + in.readInt());
                    System.out.print("],columnType:[" + in.readInt());
                    System.out.print("]\n");
                }
                System.out.println("**********RSMeta**********");
                System.out.println("**********RSRow**********");
                while (true) {
                    byte cmd = in.readByte();
                    if (cmd == (byte) 0x7e) {
                        for (int i = 0; i < colCount; i++) {
                            System.out.print("val:[" + readIntString(in));
                            if (i != colCount - 1) System.out.print("],");
                            else System.out.print("]");
                        }
                        System.out.println();
                    } else if (cmd == (byte) 0x7f) break;
                    else throw new IOException("cmd is not defined[" + cmd + "]");
                }
                System.out.println("**********RSRow**********");

                try {
                    Thread.sleep(60 * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                while (true) {
                    methodName = "next".getBytes(StandardCharsets.UTF_8);
                    tempBuffer.put((byte) 0x06);
                    tempBuffer.putShort((short) stmt.length);
                    tempBuffer.put(stmt);
                    byte[] rs = rsId.getBytes(StandardCharsets.UTF_8);
                    tempBuffer.putShort((short) rs.length);
                    tempBuffer.put(rs);
                    tempBuffer.put((byte) methodName.length);
                    tempBuffer.put(methodName);
                    outBytes = filling(tempBuffer);
                    out.write(outBytes);
                    out.flush();
                    tempBuffer.clear();
                    result = in.readByte();
                    if (0 == result) {
                        boolean first = true;
                        byte fb = in.readByte();
                        if (fb == (byte) 0x7f) break;
                        System.out.println("**********RSRow**********");
                        while (true) {
                            byte cmd;
                            if (first) cmd = fb;
                            else cmd = in.readByte();
                            if (cmd == (byte) 0x7e) {
                                for (int i = 0; i < colCount; i++) {
                                    System.out.print("val:[" + readIntString(in));
                                    if (i != colCount - 1) System.out.print("],");
                                    else System.out.print("]");
                                }
                                System.out.println();
                            } else if (cmd == (byte) 0x7f) break;
                            else throw new IOException("cmd is not defined[" + cmd + "]");
                            first = false;
                        }
                        System.out.println("**********RSRow**********");
                    } else System.out.println("[" + rsId + "] next failure: " + readShortString(in));
                }

            } else System.out.println("executeQuery failure: " + readShortString(in));
        }

    }

    @Test
    public void testQuery() throws IOException {

        testConnect();
        DataInputStream in = new DataInputStream(socket.getInputStream());
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());

        ByteBuffer tempBuffer = ByteBuffer.allocate(1024);
        byte[] methodName, outBytes;
        short result;

        methodName = "createStatement".getBytes(StandardCharsets.UTF_8);
        tempBuffer.put((byte) 0x03);
        tempBuffer.put((byte) methodName.length);
        tempBuffer.put(methodName);
        tempBuffer.putShort((short) 0);
        tempBuffer.put((byte) 0);
        outBytes = filling(tempBuffer);
        out.write(outBytes);
        out.flush();
        tempBuffer.clear();
        result = in.readByte();
        String stmtId = null;
        if (result == 0) {
            stmtId = readShortString(in);
            System.out.println("createStatement success,stmt[" + stmtId + "]");
        } else System.out.println("create statement failure: " + readShortString(in));

        if (stmtId != null) {
            byte[] stmt = stmtId.getBytes(StandardCharsets.UTF_8);

            methodName = "setFetchSize".getBytes(StandardCharsets.UTF_8);
            tempBuffer.put((byte) 0x05);
            tempBuffer.putShort((short) stmt.length);
            tempBuffer.put(stmt);
            tempBuffer.put((byte) methodName.length);
            tempBuffer.put(methodName);
            tempBuffer.putInt(2);
            outBytes = filling(tempBuffer);
            out.write(outBytes);
            out.flush();
            tempBuffer.clear();
            result = in.readByte();
            if (0 == result) System.out.println("setFetchSize success");
            else System.out.println("setFetchSize failure: " + readShortString(in));

            methodName = "executeQuery".getBytes(StandardCharsets.UTF_8);
            byte[] sql = "select * from lgjob".getBytes(StandardCharsets.UTF_8);
            tempBuffer.put((byte) 0x05);
            tempBuffer.putShort((short) stmt.length);
            tempBuffer.put(stmt);
            tempBuffer.put((byte) methodName.length);
            tempBuffer.put(methodName);
            tempBuffer.putInt(sql.length);
            tempBuffer.put(sql);
            outBytes = filling(tempBuffer);
            out.write(outBytes);
            out.flush();
            tempBuffer.clear();
            result = in.readByte();
            if (result == 0) {
                System.out.println("executeQuery success");
                String rsId = readShortString(in);
                assert rsId != null;
                System.out.println("resultSet: " + rsId);
                System.out.println("**********RSMeta**********");
                short colCount = in.readShort();
                for (int i = 0; i < colCount; i++) {
                    System.out.print("catalogName:[" + readShortString(in));
                    System.out.print("],schemaName:[" + readShortString(in));
                    System.out.print("],tableName:[" + readShortString(in));
                    System.out.print("],columnLabel:[" + readShortString(in));
                    System.out.print("],columnName:[" + readShortString(in));
                    System.out.print("],columnTypeName:[" + readShortString(in));
                    System.out.print("],columnDisplaySize:[" + in.readInt());
                    System.out.print("],precision:[" + in.readInt());
                    System.out.print("],scale:[" + in.readInt());
                    System.out.print("],columnType:[" + in.readInt());
                    System.out.print("]\n");
                }
                System.out.println("**********RSMeta**********");
                System.out.println("**********RSRow**********");
                while (true) {
                    byte cmd = in.readByte();
                    if (cmd == (byte) 0x7e) {
                        for (int i = 0; i < colCount; i++) {
                            System.out.print("val:[" + readIntString(in));
                            if (i != colCount - 1) System.out.print("],");
                            else System.out.print("]");
                        }
                        System.out.println();
                    } else if (cmd == (byte) 0x7f) break;
                    else throw new IOException("cmd is not defined[" + cmd + "]");
                }
                System.out.println("**********RSRow**********");

                while (true) {
                    methodName = "next".getBytes(StandardCharsets.UTF_8);
                    tempBuffer.put((byte) 0x06);
                    tempBuffer.putShort((short) stmt.length);
                    tempBuffer.put(stmt);
                    byte[] rs = rsId.getBytes(StandardCharsets.UTF_8);
                    tempBuffer.putShort((short) rs.length);
                    tempBuffer.put(rs);
                    tempBuffer.put((byte) methodName.length);
                    tempBuffer.put(methodName);
                    outBytes = filling(tempBuffer);
                    out.write(outBytes);
                    out.flush();
                    tempBuffer.clear();
                    result = in.readByte();
                    if (0 == result) {
                        boolean first = true;
                        byte fb = in.readByte();
                        if (fb == (byte) 0x7f) break;
                        System.out.println("**********RSRow**********");
                        while (true) {
                            byte cmd;
                            if (first) cmd = fb;
                            else cmd = in.readByte();
                            if (cmd == (byte) 0x7e) {
                                for (int i = 0; i < colCount; i++) {
                                    System.out.print("val:[" + readIntString(in));
                                    if (i != colCount - 1) System.out.print("],");
                                    else System.out.print("]");
                                }
                                System.out.println();
                            } else if (cmd == (byte) 0x7f) break;
                            else throw new IOException("cmd is not defined[" + cmd + "]");
                            first = false;
                        }
                        System.out.println("**********RSRow**********");
                    } else System.out.println("[" + rsId + "] next failure: " + readShortString(in));
                }

            } else System.out.println("executeQuery failure: " + readShortString(in));

            methodName = "executeUpdate".getBytes(StandardCharsets.UTF_8);
            tempBuffer.put((byte) 0x05);
            tempBuffer.putShort((short) stmt.length);
            tempBuffer.put(stmt);
            tempBuffer.put((byte) methodName.length);
            tempBuffer.put(methodName);
            tempBuffer.put((byte) 0x01);
            sql = "delete from test where name='test2'".getBytes(StandardCharsets.UTF_8);
            tempBuffer.putInt(sql.length);
            tempBuffer.put(sql);
            outBytes = filling(tempBuffer);
            out.write(outBytes);
            out.flush();
            tempBuffer.clear();
            result = in.readByte();
            if (0 == result) System.out.println("executeUpdate success[" + in.readInt() + "]");
            else System.out.println("executeUpdate failure: " + readShortString(in));
        }
    }

    @Test
    public void testExecute() throws IOException {

        testConnect();
        DataInputStream in = new DataInputStream(socket.getInputStream());
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());

        byte[] methodName = "createStatement".getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.put((byte) 0x03);
        buffer.put((byte) methodName.length);
        buffer.put(methodName);
        buffer.put((byte) 0);
        byte[] bytes = filling(buffer);
        out.write(bytes);
        out.flush();
        buffer.clear();
        short result = in.readByte();
        String stmtId = null;
        if (result == 0) {
            stmtId = readShortString(in);
            System.out.println("createStatement success,stmt[" + stmtId + "]");
        } else System.out.println("create statement failure: " + readShortString(in));

        if (stmtId != null) {
            execute(stmtId,
                    "create table test (name varchar(200))".getBytes(StandardCharsets.UTF_8),
                    buffer, in, out);
            execute(stmtId,
                    "insert into test values ('test')".getBytes(StandardCharsets.UTF_8),
                    buffer, in, out);
            execute(stmtId,
                    "insert into test values ('test2')".getBytes(StandardCharsets.UTF_8),
                    buffer, in, out);
            execute(stmtId,
                    "insert into test values ('test3')".getBytes(StandardCharsets.UTF_8),
                    buffer, in, out);
        }
    }

    private void execute(String stmtId, byte[] sql,
                         ByteBuffer buffer, DataInputStream in, DataOutputStream out)
            throws IOException {
        byte[] stmt = stmtId.getBytes(StandardCharsets.UTF_8);
        byte[] methodName = "execute".getBytes(StandardCharsets.UTF_8);
        buffer.put((byte) 0x05);
        buffer.putShort((short) stmt.length);
        buffer.put(stmt);
        buffer.put((byte) methodName.length);
        buffer.put(methodName);
        buffer.put((byte) 0x01);
        buffer.putInt(sql.length);
        buffer.put(sql);
        byte[] bytes = filling(buffer);
        out.write(bytes);
        out.flush();
        buffer.clear();
        short result = in.readByte();
        if (result == 0) {
            System.out.println("execute success");
            System.out.println("execute result: " + readShortString(in));
        } else System.out.println("execute failure: " + readShortString(in));
    }


    private String readShortString(DataInputStream in) throws IOException {
        short length = in.readShort();
        if (length == ~0) return null;
        byte[] bytes = new byte[length];
        in.read(bytes, 0, length);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private String readIntString(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length == ~0) return null;
        byte[] bytes = new byte[length];
        in.read(bytes, 0, length);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Test
    public void convertInt2Short() {
        int i = 200;
        short s = int2Short(i);
        assertEquals(s, (short) 200);
    }

    public static byte[] int2Bytes(int a) {

        byte[] b = new byte[4];
        b[0] = (byte) (a >> 24);
        b[1] = (byte) (a >> 16);
        b[2] = (byte) (a >> 8);
        b[3] = (byte) (a);

        return b;
    }

    public static int bytes2Int(byte[] b) {
        return ((b[0] & 0xff) << 24) | ((b[1] & 0xff) << 16)
                | ((b[2] & 0xff) << 8) | (b[3] & 0xff);
    }

    public static short int2Short(int i) {
        byte[] b = int2Bytes(i);
        return (short) (((b[2] & 0xff) << 8) | (b[3] & 0xff));
    }
}
