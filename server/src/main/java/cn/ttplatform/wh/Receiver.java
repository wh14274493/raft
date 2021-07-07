package cn.ttplatform.wh;

import cn.ttplatform.wh.support.CoreChannelInitializer;
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
public class Receiver {

    private final GlobalContext globalContext;
    private final NioEventLoopGroup boss;
    private final NioEventLoopGroup worker;
    private final int port;

    public Receiver(GlobalContext globalContext) {
        this.globalContext = globalContext;
        this.boss = globalContext.getBoss();
        this.worker = globalContext.getWorker();
        this.port = globalContext.getProperties().getPort();
    }

    public void listen() {
        ServerBootstrap serverBootstrap = new ServerBootstrap().group(boss, worker)
            .channel(NioServerSocketChannel.class)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childHandler(new CoreChannelInitializer(globalContext));
        try {
            serverBootstrap.bind(port).addListener(future -> {
                if (future.isSuccess()) {
                    log.info("server start in {}", port);
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
