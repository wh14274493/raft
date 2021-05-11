package cn.ttplatform.wh.core.listener.nio;

import cn.ttplatform.wh.core.GlobalContext;
import cn.ttplatform.wh.core.support.ChannelPool;
import cn.ttplatform.wh.core.support.CoreChannelInitializer;
import cn.ttplatform.wh.core.listener.Listener;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/22 16:39
 */
@Slf4j
public class NioListener implements Listener {

    private final GlobalContext globalContext;
    private final EventLoopGroup boss;
    private final EventLoopGroup worker;
    private final int port;

    public NioListener(GlobalContext globalContext) {
        this.globalContext = globalContext;
        this.boss = globalContext.getBoss();
        this.worker = globalContext.getWorker();
        this.port = globalContext.getProperties().getPort();
    }

    @Override
    public void listen() {
        ServerBootstrap serverBootstrap = new ServerBootstrap().group(boss, worker)
            .channel(NioServerSocketChannel.class)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childHandler(new CoreChannelInitializer(globalContext));
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
