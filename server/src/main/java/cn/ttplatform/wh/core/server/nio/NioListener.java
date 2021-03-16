package cn.ttplatform.wh.core.server.nio;

import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.core.common.MessageContext;
import cn.ttplatform.wh.core.server.Listener;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/22 16:39
 */
@Slf4j
public class NioListener implements Listener {

    private final MessageContext messageContext;
    private final EventLoopGroup boss;
    private final EventLoopGroup worker;
    private final int port;

    public NioListener(MessageContext messageContext) {
        this.messageContext = messageContext;
        ServerProperties properties = messageContext.getProperties();
        this.boss = new NioEventLoopGroup(properties.getServerListenThreads());
        this.worker = new NioEventLoopGroup(properties.getServerWorkerThreads());
        this.port = properties.getListeningPort();
    }

    @Override
    public void listen() {
        ServerBootstrap serverBootstrap = new ServerBootstrap().group(boss, worker)
            .channel(NioServerSocketChannel.class)
            .childHandler(new ServerChannelInitializer(messageContext));
        try {
            serverBootstrap.bind(port).addListener(future -> {
                if (future.isSuccess()) {
                    log.info("Connector start in {}", port);
                }
            }).sync();
        } catch (Exception e) {
            stop();
        }
    }

    @Override
    public void stop() {
        boss.shutdownGracefully();
        worker.shutdownGracefully();
    }
}
