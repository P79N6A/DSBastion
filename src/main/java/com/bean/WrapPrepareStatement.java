package com.bean;

import com.handler.CipherHandler;
import io.netty.channel.ChannelHandlerContext;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Instant;
import java.util.Map;

import static com.handler.IOHandler.OK;
import static com.handler.IOHandler.writeShortStr;

public class WrapPrepareStatement extends WrapStatement {

    private final PreparedStatement preparedStatement;
    private final Map<Integer, MaskBean> pstmtMask;
    private final SQLStruct sqlStruct;

    WrapPrepareStatement(WrapConnect wrapConnect, String id, String user, PreparedStatement statement,
                         Map<Integer, MaskBean> pstmtMask,SQLStruct sqlStruct) {
        super(wrapConnect, id, user, statement);
        this.preparedStatement = statement;
        this.pstmtMask = pstmtMask;
        this.sqlStruct = sqlStruct;
    }

    public void executeQuery(ChannelHandlerContext out) throws SQLException {
        ResultSet rs = this.preparedStatement.executeQuery();
        String rsId = this.id + COUNTER.incrementAndGet();
        WrapResultSet wrs = new WrapResultSet(this, rsId, rs, sqlStruct);
        rsMap.put(rsId, wrs);
        out.write(writeShortStr(OK, rsId));
        wrs.getMetaData(out);
        wrs.next(true, out);
    }

    public int executeUpdate() throws SQLException {
        return this.preparedStatement.executeUpdate();
    }

    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        this.preparedStatement.setNull(parameterIndex, sqlType);
    }

    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        this.preparedStatement.setBoolean(parameterIndex, x);
    }

    public void setByte(int parameterIndex, byte x) throws SQLException {
        this.preparedStatement.setByte(parameterIndex, x);
    }

    public void setShort(int parameterIndex, short x) throws SQLException {
        this.preparedStatement.setShort(parameterIndex, x);
    }

    public void setInt(int parameterIndex, int x) throws SQLException {
        this.preparedStatement.setInt(parameterIndex, x);
    }

    public void setLong(int parameterIndex, long x) throws SQLException {
        this.preparedStatement.setLong(parameterIndex, x);
    }

    public void setFloat(int parameterIndex, float x) throws SQLException {
        this.preparedStatement.setFloat(parameterIndex, x);
    }

    public void setDouble(int parameterIndex, double x) throws SQLException {
        this.preparedStatement.setDouble(parameterIndex, x);
    }


    public void setString(int parameterIndex, String x) throws Exception {
        if (x != null && pstmtMask.containsKey(parameterIndex)) x = new String(CipherHandler.encrypt(
                x.getBytes(StandardCharsets.UTF_8), pstmtMask.get(parameterIndex)),
                StandardCharsets.UTF_8);
        this.preparedStatement.setString(parameterIndex, x);
    }

    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        this.preparedStatement.setBytes(parameterIndex, x);
    }

    public void setDate(int parameterIndex, long milliseconds) throws SQLException {
        this.preparedStatement.setDate(parameterIndex, new Date(milliseconds));
    }

    public void setTime(int parameterIndex, long milliseconds) throws SQLException {
        this.preparedStatement.setTime(parameterIndex, new Time(milliseconds));
    }

    public void setTimestamp(int parameterIndex, long epochSecond, int nanos) throws SQLException {
        Timestamp ts = Timestamp.from(Instant.ofEpochSecond(epochSecond, nanos));
        this.preparedStatement.setTimestamp(parameterIndex, ts);
    }

    public void clearParameters() throws SQLException {
        this.preparedStatement.clearParameters();
    }


    public boolean execute() throws SQLException {
        return this.preparedStatement.execute();
    }

    public void addBatch() throws SQLException {
        this.preparedStatement.addBatch();
    }

    public void addBatch(String sql) throws SQLException {
        this.preparedStatement.addBatch(sql);
    }

    public int[] executeBatch() throws SQLException {
        return this.preparedStatement.executeBatch();
    }

    public void setNString(int parameterIndex, String value) throws Exception {
        if (value != null && pstmtMask.containsKey(parameterIndex)) value = new String(CipherHandler.encrypt(
                value.getBytes(StandardCharsets.UTF_8), pstmtMask.get(parameterIndex)),
                StandardCharsets.UTF_8);
        this.preparedStatement.setNString(parameterIndex, value);
    }

}
