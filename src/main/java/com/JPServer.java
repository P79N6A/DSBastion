package com;

import com.audit.AuditManager;
import com.bean.WrapConnect;
import com.util.Constants;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;


public class JPServer {

    private final EventLoopGroup mainG = new NioEventLoopGroup(1);
    private final EventLoopGroup workerG = new NioEventLoopGroup();

    private JPServer() {
    }

    public static void main(String[] args) throws InterruptedException {
        // Configure the server.
        final JPServer jpServer = new JPServer();
        final JPServerHandler serverHandler = new JPServerHandler();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(jpServer.mainG, jpServer.workerG)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(serverHandler);
                        }
                    });

            AuditManager.getInstance().start();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                serverHandler.connects.asMap().values().forEach(WrapConnect::close);
                serverHandler.connects.invalidateAll();
                AuditManager.getInstance().stop();
                jpServer.mainG.shutdownGracefully();
                jpServer.workerG.shutdownGracefully();
            }));
            // Start the server.
            ChannelFuture f = bootstrap.bind(Constants.proxyPort).sync();
            // Wait until the server socket is closed.
            f.channel().closeFuture().sync();
        } finally {
            // Shut down all event loops to terminate all threads.
            jpServer.mainG.shutdownGracefully();
            jpServer.workerG.shutdownGracefully();
        }
    }
}
