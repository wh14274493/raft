package cn.ttplatform.wh.demo;

import cn.ttplatform.wh.cmd.*;
import cn.ttplatform.wh.cmd.factory.*;
import cn.ttplatform.wh.support.DistributableCodec;
import cn.ttplatform.wh.support.DistributableFactoryRegistry;
import cn.ttplatform.wh.support.FixedSizeLinkedBufferPool;
import cn.ttplatform.wh.support.Pool;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.protostuff.LinkedBuffer;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * @author Wang Hao
 * @date 2021/4/12 22:50
 */
@Slf4j
public class Client {

    static class Watcher {

        AtomicInteger index = new AtomicInteger();
        int target;
        long start;
        long end;

        public void startWatch() {
            start = System.nanoTime();
        }

        public void stopWatch() {
            end = System.nanoTime();
        }

        public void reset(int target) {
            this.target = target;
            this.index.set(0);
        }

        public long cost() {
            return end - start;
        }

        public synchronized void increment() {
            int v = index.incrementAndGet();
            if (v >= target) {
                stopWatch();
                notifyAll();
            }
        }

        public synchronized void waitResult() throws InterruptedException {

            while (index.get() < target) {
                wait();
            }
            log.info("receive {} times resp cost {} ns.", target, cost());
        }
    }

    private final Bootstrap bootstrap;
    private final NioEventLoopGroup worker;

    public Client(Watcher watcher) {
        this.worker = new NioEventLoopGroup(1);
        Pool<LinkedBuffer> bufferPool = new FixedSizeLinkedBufferPool(3);
        DistributableFactoryRegistry factoryManager = new DistributableFactoryRegistry();
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
                .group(worker)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, Boolean.FALSE)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new DistributableCodec(factoryManager));
                        pipeline.addLast(new ClientDuplexChannelHandler(watcher));
                    }
                });
    }

    public Channel connect() throws InterruptedException {
        return bootstrap.connect("127.0.0.1", 6666).sync().channel();
    }

    public void close() {
        worker.shutdownGracefully();
    }

    public static void main(String[] args) throws InterruptedException {
        Watcher watcher = new Watcher();
        Client client = new Client(watcher);
        Channel connection = client.connect();

        int target = 100000;
        watcher.reset(target);
        watcher.startWatch();
        log.info("test {} times SetCommand.", target);
        testSet(connection, target, 128);
        watcher.waitResult();

        target = 100000;
        watcher.reset(target);
        watcher.startWatch();
        log.info("test {} times GetCommand.", target);
        testGet(connection, target);
        watcher.waitResult();

        client.close();
    }

    private static void clusterChangeCommand(Channel channel, Watcher watcher) throws InterruptedException {
        Set<String> newConfig = new HashSet<>();
        newConfig.add("A,127.0.0.1,6666");
        newConfig.add("B,127.0.0.1,7777");
        newConfig.add("C,127.0.0.1,8888");

        watcher.startWatch();
        channel.writeAndFlush(ClusterChangeCommand.builder().newConfig(newConfig).id(UUID.randomUUID().toString()).build());
    }

    private static void testSet(Channel channel, int times, int bodySize) {
        StringBuilder value = new StringBuilder();
        while (value.length() < bodySize) {
            value.append(UUID.randomUUID());
        }
        String v = value.substring(0, bodySize);
        String id = UUID.randomUUID().toString();
        IntStream.range(0, times).forEach(index -> channel.write(SetCommand.builder().id(id + index).keyValuePair(new KeyValuePair("test" + index, v + index)).build()));
    }

    private static void testGet(Channel channel, int times) {
        String id = UUID.randomUUID().toString();
        IntStream.range(0, times).forEach(index -> {
            GetCommand getCommand = GetCommand.builder().id(id + index).key("test" + index).build();
            channel.write(getCommand);
        });
    }

    private static SetCommand setCommand(String key, String value) {
        return SetCommand.builder().id(UUID.randomUUID().toString()).keyValuePair(new KeyValuePair(key, value)).build();
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
                SetCommand.builder().id(UUID.randomUUID().toString()).keyValuePair(new KeyValuePair("wanghao" + index, finalS)).build()));
        IntStream.range(0, 1000).forEach(index -> channel
                .writeAndFlush(GetCommand.builder().id(UUID.randomUUID().toString()).key("wanghao" + index).build()));
    }

    public static void create1000Connections(Client client) throws InterruptedException {
        List<Channel> channels = new ArrayList<>(1000);
        IntStream.range(0, 10000).forEach(index -> {
            try {
                channels.add(client.connect());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        log.info("start at {}", System.nanoTime());
        channels.forEach(channel -> channel.writeAndFlush(GetCommand.builder().id(UUID.randomUUID().toString()).key("wanghao1").build()));
        TimeUnit.MILLISECONDS.sleep(10);
    }
}
