package cn.ttplatform.wh.server.nio;

import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.support.Listener;
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

    private final NodeContext nodeContext;
    private final EventLoopGroup boss;
    private final EventLoopGroup worker;
    private final int port;

    public NioListener(NodeContext nodeContext, EventLoopGroup boss, EventLoopGroup worker) {
        this.nodeContext = nodeContext;
        ServerProperties properties = nodeContext.getProperties();
        this.boss = boss;
        this.worker = worker;
        this.port = properties.getPort();
    }

    @Override
    public void listen() {
        ServerBootstrap serverBootstrap = new ServerBootstrap().group(boss, worker)
            .channel(NioServerSocketChannel.class)
            .childHandler(new ServerChannelInitializer(nodeContext));
        try {
            serverBootstrap.bind(port).addListener(future -> {
                if (future.isSuccess()) {
                    log.info("Listener start in {}", port);
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
