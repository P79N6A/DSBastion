package com.bean;

import com.handler.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static com.handler.IOHandler.*;

public class WrapResultSet implements AutoCloseable {

    private final String id;
    private final WrapStatement wrapStatement;
    private final ResultSet resultSet;

    private SQLStruct sqlStruct;  //maybe null[statement.getGeneratedKeys()]

    WrapResultSet(WrapStatement wrapStatement, String id, ResultSet resultSet, SQLStruct sqlStruct) {
        this.wrapStatement = wrapStatement;
        this.id = id;
        this.resultSet = resultSet;
        this.sqlStruct = sqlStruct;
    }

    WrapResultSet(WrapStatement wrapStatement, String id, ResultSet resultSet) {
        this.wrapStatement = wrapStatement;
        this.id = id;
        this.resultSet = resultSet;
    }

    public WrapStatement getWrapStatement() {
        return wrapStatement;
    }

    public String getCursorName() throws SQLException {
        return this.resultSet.getCursorName();
    }

    void getMetaData(ChannelHandlerContext out) throws SQLException {
        ResultSetMetaData rsMeta = this.resultSet.getMetaData();
        int colCount = rsMeta.getColumnCount();
        out.write(writeShort(colCount));
        for (int i = 1; i <= colCount; i++) {
            ByteBuf buf = Unpooled.buffer();
            writeShortString(rsMeta.getCatalogName(i), buf);
            writeShortString(rsMeta.getSchemaName(i), buf);
            writeShortString(rsMeta.getTableName(i), buf);
            writeShortString(rsMeta.getColumnLabel(i), buf);
            writeShortString(rsMeta.getColumnName(i), buf);
            writeShortString(rsMeta.getColumnTypeName(i), buf);
            buf.writeInt(rsMeta.getColumnDisplaySize(i));
            buf.writeInt(rsMeta.getPrecision(i));
            buf.writeInt(rsMeta.getScale(i));
            buf.writeInt(rsMeta.getColumnType(i));
            out.write(buf);
        }
    }

    public boolean isLast() throws SQLException {
        return this.resultSet.isLast();
    }

    public boolean isFirst() throws SQLException {
        return this.resultSet.isFirst();
    }

    public void beforeFirst() throws SQLException {
        this.resultSet.beforeFirst();
    }

    public void afterLast() throws SQLException {
        this.resultSet.afterLast();
    }

    public boolean first() throws SQLException {
        return this.resultSet.first();
    }

    public boolean last() throws SQLException {
        return this.resultSet.last();
    }

    public int getRow() throws SQLException {
        return this.resultSet.getRow();
    }

    public boolean absolute(int row) throws SQLException {
        return this.resultSet.absolute(row);
    }

    public boolean relative(int rows) throws SQLException {
        return this.resultSet.relative(rows);
    }

    public void setFetchSize(int rows) throws SQLException {
        this.resultSet.setFetchSize(rows);
    }

    private int getFetchSize() throws SQLException {
        return this.resultSet.getFetchSize();
    }

    public void next(boolean first, ChannelHandlerContext out) throws SQLException {
        ResultSetMetaData rsmd = this.resultSet.getMetaData();
        if (this.sqlStruct != null && rsmd.getColumnCount() != this.sqlStruct.getFirst().size())
            throw new SQLException("ResultSetMetaData columnCount mismatch");
        int fetchSize = getFetchSize();
        if (fetchSize == 0) fetchSize = this.wrapStatement.getFetchSize();
        if (!first) out.write(writeByte((byte) 0x00));
        while (fetchSize > 0 && this.resultSet.next()) {
            ByteBuf buf = Unpooled.buffer();
            buf.writeByte(0x7e);
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                ColStruct colStruct = null;
                if (this.sqlStruct != null) colStruct = this.sqlStruct.getFirst().get(i - 1);
                byte[] bytes;
                if (rsmd.getColumnType(i) == 2) bytes = this.resultSet.getBigDecimal(i).toPlainString()
                        .getBytes(StandardCharsets.UTF_8);
                else bytes = this.resultSet.getBytes(i);
                if (colStruct != null && colStruct.getMaskBean() != null) {
                    if (2 == colStruct.getMaskBean().getType()) {
                        try {
                            bytes = CipherHandler.decrypt(bytes, colStruct.getMaskBean());
                        } catch (Exception e) {
                            throw new SQLException(e);
                        }
                    } else bytes = MaskHandler.maskValue(bytes, colStruct.getMaskBean());
                }
                if (colStruct != null && !colStruct.isVisible()) bytes = null;

                if (bytes == null) buf.writeInt(~0);
                else {
                    buf.writeInt(bytes.length);
                    buf.writeBytes(bytes);
                }
            }
            out.write(buf);
            fetchSize--;
        }
        out.write(writeByte((byte) 0x7f));
    }

    @Override
    public void close() {
        try {
            if (resultSet != null) resultSet.close();
            wrapStatement.rsMap.remove(id);
        } catch (SQLException ignored) {
        }
    }
}
