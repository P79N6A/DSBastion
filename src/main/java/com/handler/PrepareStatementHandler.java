package com.handler;

import com.audit.AuditEvent;
import com.audit.AuditManager;
import com.bean.WrapPrepareStatement;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.sql.*;

import static com.handler.IOHandler.*;

public class PrepareStatementHandler {

    public static void handler(WrapPrepareStatement statement, ByteBuf src, ChannelHandlerContext out)
            throws Exception {
        String mName = IOHandler.readByteLen(src);
        if ("executeQuery".equals(mName)) {
            AuditManager.getInstance().audit(new AuditEvent(statement.getWrapConnect().getAddress(),
                    statement.getUser(), mName));
            statement.executeQuery(out);
        } else if ("executeUpdate".equals(mName)) {
            AuditManager.getInstance().audit(new AuditEvent(statement.getWrapConnect().getAddress(),
                    statement.getUser(), mName));
            out.write(writeInt(OK, statement.executeUpdate()));
        } else if ("setNull".equals(mName)) {
            int i = src.readInt();
            int v = src.readInt();
            AuditManager.getInstance().audit(new AuditEvent(statement.getWrapConnect().getAddress(),
                    statement.getUser(), mName, i, v));
            statement.setNull(i, v);
            out.write(writeByte(OK));
        } else if ("setBoolean".equals(mName)) {
            int i = src.readInt();
            String v = readByteLen(src);
            AuditManager.getInstance().audit(new AuditEvent(statement.getWrapConnect().getAddress(),
                    statement.getUser(), mName, i, v));
            statement.setBoolean(i, "true".equals(v));
            out.write(writeByte(OK));
        } else if ("setByte".equals(mName)) {
            int i = src.readInt();
            byte v = src.readByte();
            AuditManager.getInstance().audit(new AuditEvent(statement.getWrapConnect().getAddress(),
                    statement.getUser(), mName, i, v));
            statement.setByte(i, v);
            out.write(writeByte(OK));
        } else if ("setShort".equals(mName)) {
            int i = src.readInt();
            short v = src.readShort();
            AuditManager.getInstance().audit(new AuditEvent(statement.getWrapConnect().getAddress(),
                    statement.getUser(), mName, i, v));
            statement.setShort(i, v);
            out.write(writeByte(OK));
        } else if ("setInt".equals(mName)) {
            int i = src.readInt();
            int v = src.readInt();
            AuditManager.getInstance().audit(new AuditEvent(statement.getWrapConnect().getAddress(),
                    statement.getUser(), mName, i, v));
            statement.setInt(i, v);
            out.write(writeByte(OK));
        } else if ("setLong".equals(mName)) {
            int i = src.readInt();
            long v = src.readLong();
            AuditManager.getInstance().audit(new AuditEvent(statement.getWrapConnect().getAddress(),
                    statement.getUser(), mName, i, v));
            statement.setLong(i, v);
            out.write(writeByte(OK));
        } else if ("setFloat".equals(mName)) {
            int i = src.readInt();
            float v = src.readFloat();
            AuditManager.getInstance().audit(new AuditEvent(statement.getWrapConnect().getAddress(),
                    statement.getUser(), mName, i, v));
            statement.setFloat(i, v);
            out.write(writeByte(OK));
        } else if ("setDouble".equals(mName)) {
            int i = src.readInt();
            double v = src.readDouble();
            AuditManager.getInstance().audit(new AuditEvent(statement.getWrapConnect().getAddress(),
                    statement.getUser(), mName, i, v));
            statement.setDouble(i, v);
            out.write(writeByte(OK));
        } else if ("setString".equals(mName)) {
            int i = src.readInt();
            String v = readIntLen(src);
            AuditManager.getInstance().audit(new AuditEvent(statement.getWrapConnect().getAddress(),
                    statement.getUser(), mName, i, v));
            statement.setString(i, v);
            out.write(writeByte(OK));
        } else if ("setBytes".equals(mName)) {
            int i = src.readInt();
            int v = src.readInt();
            AuditManager.getInstance().audit(new AuditEvent(statement.getWrapConnect().getAddress(),
                    statement.getUser(), mName, i, "bytes length[" + v + "]"));
            statement.setBytes(i, readBytes(v, src));
            out.write(writeByte(OK));
        } else if ("setDate".equals(mName)) {
            int i = src.readInt();
            long v = src.readLong();
            AuditManager.getInstance().audit(new AuditEvent(statement.getWrapConnect().getAddress(),
                    statement.getUser(), mName, i, v));
            statement.setDate(i, v);
            out.write(writeByte(OK));
        } else if ("setTime".equals(mName)) {
            int i = src.readInt();
            long v = src.readLong();
            AuditManager.getInstance().audit(new AuditEvent(statement.getWrapConnect().getAddress(),
                    statement.getUser(), mName, i, v));
            statement.setTime(src.readInt(), src.readLong());
            out.write(writeByte(OK));
        } else if ("setTimestamp".equals(mName)) {
            int i = src.readInt();
            long v = src.readLong();
            int v1 = src.readInt();
            AuditManager.getInstance().audit(new AuditEvent(statement.getWrapConnect().getAddress(),
                    statement.getUser(), mName, i, v, v1));
            statement.setTimestamp(i, v, v1);
            out.write(writeByte(OK));
        } else if ("clearParameters".equals(mName)) {
            AuditManager.getInstance().audit(new AuditEvent(statement.getWrapConnect().getAddress(),
                    statement.getUser(), mName));
            statement.clearParameters();
            out.write(writeByte(OK));
        } else if ("execute".equals(mName)) {
            AuditManager.getInstance().audit(new AuditEvent(statement.getWrapConnect().getAddress(),
                    statement.getUser(), mName));
            out.write(writeShortStr(OK, statement.execute()));
        } else if ("addBatch".equals(mName)) {
            short pc = src.readByte();
            if (0 == pc) {
                AuditManager.getInstance().audit(new AuditEvent(statement.getWrapConnect().getAddress(),
                        statement.getUser(), mName));
                statement.addBatch();
            } else if (1 == pc) {
                String sql = readIntLen(src);
                AuditManager.getInstance().audit(new AuditEvent(statement.getWrapConnect().getAddress(),
                        statement.getUser(), mName, sql));
                statement.addBatch(sql);
            } else throw new SQLException("addBatch param count[" + pc + "] is not exit");
            out.write(writeByte(OK));
        } else if ("setNString".equals(mName)) {
            int i = src.readInt();
            String v = readIntLen(src);
            AuditManager.getInstance().audit(new AuditEvent(statement.getWrapConnect().getAddress(),
                    statement.getUser(), mName, i, v));
            statement.setNString(i, v);
            out.write(writeByte(OK));
        } else if ("executeBatch".equals(mName)) {
            AuditManager.getInstance().audit(new AuditEvent(statement.getWrapConnect().getAddress(),
                    statement.getUser(), mName));
            out.write(writeInt(OK, statement.executeBatch()));
        } else throw new SQLException("prepareStatementMethod[" + mName + "] is not support");
    }

}
