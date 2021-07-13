package cn.ttplatform.wh;

import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.support.ServerChannelInitializer;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/22 16:39
 */
@Slf4j
public class Server {

    private final GlobalContext context;
    private final NioEventLoopGroup boss;
    private final NioEventLoopGroup worker;

    public Server(GlobalContext context) {
        this.context = context;
        this.boss = context.getBoss();
        this.worker = context.getWorker();
    }

    public void listen() {
        ServerBootstrap serverBootstrap = new ServerBootstrap().group(boss, worker)
                .channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.TCP_NODELAY, false)
                .childHandler(new ServerChannelInitializer(context));
        ServerProperties properties = context.getProperties();
        String host = properties.getHost();
        int port = properties.getPort();
        try {
            serverBootstrap.bind(host, port).addListener(future -> {
                if (future.isSuccess()) {
                    log.info("server start in {}:{}", host, port);
                }
            }).sync();
        } catch (InterruptedException e) {
            log.error("failed to start server.");
            stop();
            Thread.currentThread().interrupt();
        }
    }

    public void stop() {
        boss.shutdownGracefully();
        worker.shutdownGracefully();
    }
}
