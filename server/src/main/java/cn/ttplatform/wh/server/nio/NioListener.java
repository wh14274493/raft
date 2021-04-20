package cn.ttplatform.wh.server.nio;

import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.constant.MessageType;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.support.Listener;
import cn.ttplatform.wh.core.support.MessageDispatcher;
import cn.ttplatform.wh.core.support.MessageFactoryManager;
import cn.ttplatform.wh.cmd.factory.GetCommandFactory;
import cn.ttplatform.wh.cmd.factory.GetResponseCommandFactory;
import cn.ttplatform.wh.cmd.factory.SetCommandFactory;
import cn.ttplatform.wh.cmd.factory.SetResponseCommandFactory;
import cn.ttplatform.wh.server.command.handler.GetCommandHandler;
import cn.ttplatform.wh.server.command.handler.SetCommandHandler;
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
    private final MessageDispatcher commandDispatcher;
    private final MessageFactoryManager factoryManager;

    public NioListener(NodeContext nodeContext) {
        this.nodeContext = nodeContext;
        this.commandDispatcher = new MessageDispatcher();
        this.factoryManager = new MessageFactoryManager();
        ServerProperties properties = nodeContext.config();
        this.boss = new NioEventLoopGroup(properties.getServerListenThreads());
        this.worker = new NioEventLoopGroup(properties.getServerWorkerThreads());
        this.port = properties.getListeningPort();
    }

    private void initCommandHandler(NodeContext context) {
        commandDispatcher.register(MessageType.SET_COMMAND, new SetCommandHandler(context.node()));
        log.info("register SetCommandHandler[{}] for Listener", MessageType.SET_COMMAND);
        commandDispatcher.register(MessageType.GET_COMMAND, new GetCommandHandler(context.node()));
        log.info("register GetCommandHandler[{}] for Listener", MessageType.GET_COMMAND);
    }

    private void initFactoryManager(NodeContext context) {
        factoryManager.register(MessageType.SET_COMMAND_RESPONSE, new SetResponseCommandFactory(context.bufferPool()));
        log.info("register SetResponseCommandFactory[{}] for Listener", MessageType.SET_COMMAND_RESPONSE);
        factoryManager.register(MessageType.GET_COMMAND_RESPONSE, new GetResponseCommandFactory(context.bufferPool()));
        log.info("register GetResponseCommandFactory[{}] for Listener", MessageType.GET_COMMAND_RESPONSE);
        factoryManager.register(MessageType.SET_COMMAND, new SetCommandFactory(context.bufferPool()));
        log.info("register SetCommandFactory[{}] for Listener", MessageType.SET_COMMAND);
        factoryManager.register(MessageType.GET_COMMAND, new GetCommandFactory(context.bufferPool()));
        log.info("register GetCommandFactory[{}] for Listener", MessageType.SET_COMMAND);
    }

    @Override
    public void listen() {
        initCommandHandler(nodeContext);
        initFactoryManager(nodeContext);
        ServerBootstrap serverBootstrap = new ServerBootstrap().group(boss, worker)
            .channel(NioServerSocketChannel.class)
            .childHandler(new ServerChannelInitializer(nodeContext, commandDispatcher, factoryManager));
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
