package cn.ttplatform.wh;

import cn.ttplatform.wh.cmd.ClusterChangeCommand;
import cn.ttplatform.wh.cmd.Command;
import cn.ttplatform.wh.cmd.GetClusterInfoCommand;
import cn.ttplatform.wh.cmd.GetCommand;
import cn.ttplatform.wh.cmd.SetCommand;
import cn.ttplatform.wh.cmd.factory.ClusterChangeCommandFactory;
import cn.ttplatform.wh.cmd.factory.ClusterChangeResultCommandFactory;
import cn.ttplatform.wh.cmd.factory.GetClusterInfoCommandFactory;
import cn.ttplatform.wh.cmd.factory.GetClusterInfoResultCommandFactory;
import cn.ttplatform.wh.cmd.factory.GetCommandFactory;
import cn.ttplatform.wh.cmd.factory.GetResultCommandFactory;
import cn.ttplatform.wh.cmd.factory.RedirectCommandFactory;
import cn.ttplatform.wh.cmd.factory.RequestFailedCommandFactory;
import cn.ttplatform.wh.cmd.factory.SetCommandFactory;
import cn.ttplatform.wh.cmd.factory.SetResultCommandFactory;
import cn.ttplatform.wh.support.ByteArrayPool;
import cn.ttplatform.wh.support.Pool;
import cn.ttplatform.wh.support.DistributableCodec;
import cn.ttplatform.wh.support.DistributableFactoryManager;
import cn.ttplatform.wh.support.FixedSizeLinkedBufferPool;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.protostuff.LinkedBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/4/12 22:50
 */
@Slf4j
public class Client {

    private final Bootstrap bootstrap;
    private final Map<String, Command> pending = new ConcurrentHashMap<>();

    public Client() {
        Pool<byte[]> byteArrayPool = new ByteArrayPool(10, 1024 * 1024 * 10);
        Pool<LinkedBuffer> bufferPool = new FixedSizeLinkedBufferPool(3);
        DistributableFactoryManager factoryManager = new DistributableFactoryManager();
        factoryManager.register(new GetClusterInfoResultCommandFactory(bufferPool));
        factoryManager.register(new GetClusterInfoCommandFactory(bufferPool));
        factoryManager.register(new RedirectCommandFactory(bufferPool));
        factoryManager.register(new RequestFailedCommandFactory(bufferPool));
        factoryManager.register(new ClusterChangeCommandFactory(bufferPool));
        factoryManager.register(new ClusterChangeResultCommandFactory(bufferPool));
        factoryManager.register(new SetCommandFactory(bufferPool));
        factoryManager.register(new GetCommandFactory(bufferPool));
        factoryManager.register(new SetResultCommandFactory(bufferPool));
        factoryManager.register(new GetResultCommandFactory(bufferPool));
        bootstrap = new Bootstrap()
            .group(new NioEventLoopGroup(1))
            .channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, Boolean.TRUE)
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();
                    pipeline.addLast(new DistributableCodec(factoryManager, byteArrayPool));
                    pipeline.addLast(new ChannelDuplexHandler() {
                        int index = 1;

                        @Override
                        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                            if (index == 1) {
                                log.debug(System.nanoTime() + "");
                                log.info(msg.toString());
                            }
                            if (index == 10000) {
                                log.debug(System.nanoTime() + "");
                                log.info(msg.toString());
                            }
                            index++;
//                            log.info(msg.toString());
                        }
                    });
                }
            });
    }

    public void send(Command command) throws InterruptedException {
        Channel channel = connect();
        channel.writeAndFlush(command);
    }

    public Channel connect() throws InterruptedException {
        return bootstrap.connect("127.0.0.1", 6666).sync().channel();
    }

    public static void main(String[] args) throws InterruptedException {
        Client client = new Client();
//        client.send(clusterChangeCommand());
//        client.send(getClusterInfoCommand());
        Channel channel = client.connect();

        IntStream.range(0, 10000).forEach(index -> {
            StringBuilder value = new StringBuilder();
            while (value.length() < 256) {
                value.append(UUID.randomUUID().toString());
            }
            String s = value.substring(0, 256);
            String id = UUID.randomUUID().toString();
            SetCommand setCommand = SetCommand.builder().id(id).key("wanghao1212" + index).value(s + "|" + index).build();
            channel.writeAndFlush(setCommand);
            if (index == 0) {
                log.info("begin:" + System.nanoTime());
            }
            try {
                TimeUnit.MILLISECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
//        IntStream.range(100000, 110000).forEach(index -> channel
//            .writeAndFlush(GetCommand.builder().id(UUID.randomUUID().toString()).key("wanghao" + index).build()));
    }

    private static ClusterChangeCommand clusterChangeCommand() {
        Set<String> newConfig = new HashSet<>();
        newConfig.add("A,127.0.0.1,6666");
        newConfig.add("B,127.0.0.1,7777");
        newConfig.add("C,127.0.0.1,8888");
        newConfig.add("D,127.0.0.1,9999");
//        newConfig.add("E,127.0.0.1,5555");
        return ClusterChangeCommand.builder().newConfig(newConfig)
            .id(UUID.randomUUID().toString())
            .build();
    }

    private static SetCommand setCommand(String key, String value) {
        return SetCommand.builder().id(UUID.randomUUID().toString()).key(key).value(value).build();
    }

    private static GetCommand getCommand(String key) {
        return GetCommand.builder().id(UUID.randomUUID().toString()).key(key).build();
    }

    private static GetClusterInfoCommand getClusterInfoCommand() {
        return GetClusterInfoCommand.builder().id(UUID.randomUUID().toString()).build();
    }

    public static void test(Channel channel) {
        String s = "";
        while (s.length() < 256) {
            s += UUID.randomUUID().toString();
        }
        s = s.substring(0, 256);
        log.debug(" START {}", System.currentTimeMillis());
        String finalS = s;
        String keyPrefix = UUID.randomUUID().toString();
        IntStream.range(0, 10000).forEach(index -> channel.writeAndFlush(
            SetCommand.builder().id(UUID.randomUUID().toString())
                .key("wanghao" + index)
                .value(finalS)
                .build()));
        IntStream.range(0, 1000).forEach(index -> channel
            .writeAndFlush(GetCommand.builder().id(UUID.randomUUID().toString()).key("wanghao" + index).build()));
    }
}
