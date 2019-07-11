package com;

import com.bean.*;
import com.exception.PermissionException;
import com.google.common.cache.*;
import com.handler.*;
import com.util.Constants;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.log4j.Logger;

import java.util.concurrent.*;

import static com.handler.IOHandler.*;


@ChannelHandler.Sharable
public class JPServerHandler extends ChannelInboundHandlerAdapter {

    private final Logger logger = Logger.getLogger(JPServerHandler.class);
    //<address,connect>
    final Cache<String, WrapConnect> connects = CacheBuilder.newBuilder()
            .expireAfterAccess(Constants.proxyTimeout, TimeUnit.SECONDS)
            .removalListener((RemovalListener<String, WrapConnect>) notify -> {
                if (notify.getCause() == RemovalCause.EXPIRED) {
                    notify.getValue().close();
                }
            }).build();


    @Override
    public void channelRead(ChannelHandlerContext out, Object obj) {
        String rAddress = out.channel().remoteAddress().toString();
        ByteBuf src = (ByteBuf) obj;
        boolean finish = false;
        while (!finish && src.isReadable()) {
            short cmd = src.readByte();
            try {
                switch (cmd) {
                    case ~0:
                        finish = true;
                        break;
                    case 2:
                        String ak = readShortLen(src);
                        String mac = readShortLen(src);
//                        String process = readIntLen(src);
                        WrapConnect conn = new WrapConnect(rAddress, ak);
                        if (connects.getIfPresent(rAddress) != null) closeConn(rAddress);
                        connects.put(rAddress, conn);
                        out.write(writeByte(OK));
                        break;
                    case 3:
                        WrapConnect wrapConnect = connects.getIfPresent(rAddress);
                        if (wrapConnect == null) {
                            finish = true;
                            throw new NullPointerException("connection is closed...");
                        }
                        ConnectHandler.handler(wrapConnect, src, out);
                        break;
                    case 4:
                        wrapConnect = connects.getIfPresent(rAddress);
                        if (wrapConnect == null) {
                            finish = true;
                            throw new NullPointerException("connection is closed...");
                        }
                        ConnectMetaHandler.handler(wrapConnect, src, out);
                        break;
                    case 5:
                        wrapConnect = connects.getIfPresent(rAddress);
                        if (wrapConnect == null) {
                            finish = true;
                            throw new NullPointerException("connection is closed...");
                        }
                        String stmtId = readShortLen(src);
                        WrapStatement wrapStatement = wrapConnect.getStatement(stmtId);
                        if (wrapStatement == null) {
                            finish = true;
                            throw new NullPointerException("statement[" + stmtId + "] is closed...");
                        }
                        StatementHandler.handler(wrapStatement, src, out);
                        break;
                    case 6:
                        wrapConnect = connects.getIfPresent(rAddress);
                        if (wrapConnect == null) {
                            finish = true;
                            throw new NullPointerException("connection is closed...");
                        }
                        stmtId = readShortLen(src);
                        wrapStatement = wrapConnect.getStatement(stmtId);
                        if (wrapStatement == null) {
                            finish = true;
                            throw new NullPointerException("statement[" + stmtId + "] is closed...");
                        }
                        String rsId = readShortLen(src);
                        WrapResultSet wrapResultSet = wrapStatement.getResultSet(rsId);
                        if (wrapResultSet == null) {
                            finish = true;
                            throw new NullPointerException("resultSet[" + rsId + "] is closed...");
                        }
                        ResultSetHandler.handler(wrapResultSet, src, out);
                        break;
                    case 7:
                        wrapConnect = connects.getIfPresent(rAddress);
                        if (wrapConnect == null) {
                            finish = true;
                            throw new NullPointerException("connection is closed...");
                        }
                        stmtId = readShortLen(src);
                        WrapPrepareStatement wrapPrepareStatement = wrapConnect.getPrepareStatement(stmtId);
                        if (wrapPrepareStatement == null) {
                            finish = true;
                            throw new NullPointerException("prepareStatement[" + stmtId + "] is closed...");
                        }
                        PrepareStatementHandler.handler(wrapPrepareStatement, src, out);
                        break;
                    case 8:
                        wrapConnect = connects.getIfPresent(rAddress);
                        if (wrapConnect == null) {
                            finish = true;
                            throw new NullPointerException("connection is closed...");
                        }
                        out.write(IOHandler.writeByte(OK));
                        break;
                    default:
                        logger.error("cmd[" + cmd + "] is not defined");
                }
            } catch (Exception e) {
                logger.error(rAddress, e);
                if (e instanceof PermissionException) out.write(writeShortStr(ERROR, "no permission"));
                else out.write(writeShortStr(ERROR, e.getMessage()));
            }
        }
        src.release();
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        String address = ctx.channel().remoteAddress().toString();
        closeConn(address);
    }


    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        String address = ctx.channel().remoteAddress().toString();
        logger.error(address, cause);
        closeConn(address);
        ctx.close();
    }

    private void closeConn(String key) {
        WrapConnect conn = connects.getIfPresent(key);
        if (conn != null) {
            connects.invalidate(key);
            conn.close();
        }
    }

}