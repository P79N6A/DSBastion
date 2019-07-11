package com.handler;

import com.audit.AuditEvent;
import com.audit.AuditManager;
import com.bean.WrapResultSet;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.sql.SQLException;

import static com.handler.IOHandler.*;

public class ResultSetHandler {

    public static void handler(WrapResultSet resultSet, ByteBuf src, ChannelHandlerContext out)
            throws SQLException {
        String mName = IOHandler.readByteLen(src);
        if ("getCursorName".equals(mName)) {
            AuditManager.getInstance().audit(new AuditEvent(resultSet.getWrapStatement().getWrapConnect()
                    .getAddress(), resultSet.getWrapStatement().getUser(), mName));
            out.write(writeShortStr(OK, resultSet.getCursorName()));
        } else if ("isLast".equals(mName)) {
            AuditManager.getInstance().audit(new AuditEvent(resultSet.getWrapStatement().getWrapConnect()
                    .getAddress(), resultSet.getWrapStatement().getUser(), mName));
            out.write(writeShortStr(OK, resultSet.isLast()));
        } else if ("beforeFirst".equals(mName)) {
            AuditManager.getInstance().audit(new AuditEvent(resultSet.getWrapStatement().getWrapConnect()
                    .getAddress(), resultSet.getWrapStatement().getUser(), mName));
            resultSet.beforeFirst();
            out.write(writeByte(OK));
        } else if ("afterLast".equals(mName)) {
            AuditManager.getInstance().audit(new AuditEvent(resultSet.getWrapStatement().getWrapConnect()
                    .getAddress(), resultSet.getWrapStatement().getUser(), mName));
            resultSet.afterLast();
            out.write(writeByte(OK));
        } else if ("first".equals(mName)) {
            AuditManager.getInstance().audit(new AuditEvent(resultSet.getWrapStatement().getWrapConnect()
                    .getAddress(), resultSet.getWrapStatement().getUser(), mName));
            out.write(writeShortStr(OK, resultSet.first()));
        } else if ("last".equals(mName)) {
            AuditManager.getInstance().audit(new AuditEvent(resultSet.getWrapStatement().getWrapConnect()
                    .getAddress(), resultSet.getWrapStatement().getUser(), mName));
            out.write(writeShortStr(OK, resultSet.last()));
        } else if ("getRow".equals(mName)) {
            AuditManager.getInstance().audit(new AuditEvent(resultSet.getWrapStatement().getWrapConnect()
                    .getAddress(), resultSet.getWrapStatement().getUser(), mName));
            out.write(writeInt(OK, resultSet.getRow()));
        } else if ("absolute".equals(mName)) {
            int i = src.readInt();
            AuditManager.getInstance().audit(new AuditEvent(resultSet.getWrapStatement().getWrapConnect()
                    .getAddress(), resultSet.getWrapStatement().getUser(), mName, i));
            out.write(writeShortStr(OK, resultSet.absolute(i)));
        } else if ("relative".equals(mName)) {
            int i = src.readInt();
            AuditManager.getInstance().audit(new AuditEvent(resultSet.getWrapStatement().getWrapConnect()
                    .getAddress(), resultSet.getWrapStatement().getUser(), mName, i));
            out.write(writeShortStr(OK, resultSet.relative(i)));
        } else if ("setFetchSize".equals(mName)) {
            int i = src.readInt();
            AuditManager.getInstance().audit(new AuditEvent(resultSet.getWrapStatement().getWrapConnect()
                    .getAddress(), resultSet.getWrapStatement().getUser(), mName, i));
            resultSet.setFetchSize(i);
            out.write(writeByte(OK));
        } else if ("next".equals(mName)) {
            AuditManager.getInstance().audit(new AuditEvent(resultSet.getWrapStatement().getWrapConnect()
                    .getAddress(), resultSet.getWrapStatement().getUser(), mName));
            resultSet.next(false, out);
        } else if ("close".equals(mName)) {
            AuditManager.getInstance().audit(new AuditEvent(resultSet.getWrapStatement().getWrapConnect()
                    .getAddress(), resultSet.getWrapStatement().getUser(), "resultSet=>" + mName));
            resultSet.close();
            out.write(writeByte(OK));
        } else throw new SQLException("statementMethod[" + mName + "] is not support");
    }
}
