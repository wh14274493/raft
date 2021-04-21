package cn.ttplatform.wh.core;

import cn.ttplatform.wh.cmd.Command;
import cn.ttplatform.wh.cmd.GetCommand;
import cn.ttplatform.wh.cmd.GetResponseCommand;
import cn.ttplatform.wh.cmd.SetCommand;
import cn.ttplatform.wh.cmd.SetResponseCommand;
import cn.ttplatform.wh.core.support.ConnectionPool;
import cn.ttplatform.wh.core.support.Future;
import cn.ttplatform.wh.core.support.IDGenerator;
import cn.ttplatform.wh.core.support.RequestRecord;
import java.lang.reflect.Constructor;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/4/12 22:50
 */
@Slf4j
public class Client {

    private final ClientContext context;
    private final IDGenerator idGenerator;
    private final ConnectionPool connectionPool;

    public Client(ClientContext context) {
        this.context = context;
        String idGenerateStrategy = context.getProperties().getIdGenerateStrategy();
        try {
            Class<?> clz = Class.forName(idGenerateStrategy);
            Constructor<?> constructor = clz.getConstructor();
            idGenerator = (IDGenerator) constructor.newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException(idGenerateStrategy + "not found");
        }
        connectionPool = new ConnectionPool(context);
    }

    public boolean set(String key, String value) {

        SetCommand setCommand = SetCommand.builder().id(idGenerator.generate()).key(key).value(value).build();
        Future<Command> future = connectionPool.getConnection().send(setCommand);
        SetResponseCommand resp;
        try {
            resp = (SetResponseCommand) future.get();
        } catch (Exception e) {
            throw new IllegalStateException("request had been interrupted");
        }
        return resp.isResult();
    }

    public String get(String key) {
        GetCommand getCommand = GetCommand.builder().id(idGenerator.generate()).key(key).build();
        Future<Command> future = connectionPool.getConnection().send(getCommand);
        GetResponseCommand resp;
        try {
            resp = (GetResponseCommand) future.get();
        } catch (Exception e) {
            throw new IllegalStateException("request had been interrupted");
        }
        return resp.getValue();
    }

//    public static void main(String[] args) {
//        BufferPool<LinkedBuffer> bufferPool = new FixedSizeLinkedBufferPool(3);
//        MessageFactoryManager factoryManager = new MessageFactoryManager();
//        factoryManager.register(MessageType.SET_COMMAND, new SetCommandFactory(bufferPool));
//        factoryManager.register(MessageType.GET_COMMAND, new GetCommandFactory(bufferPool));
//        factoryManager.register(MessageType.SET_COMMAND_RESPONSE, new SetResponseCommandFactory(bufferPool));
//        factoryManager.register(MessageType.GET_COMMAND_RESPONSE, new GetResponseCommandFactory(bufferPool));
//        Bootstrap bootstrap = new Bootstrap();
//        try {
//            ChannelFuture channelFuture = bootstrap.group(new NioEventLoopGroup(1))
//                .channel(NioSocketChannel.class)
//                .option(ChannelOption.TCP_NODELAY, Boolean.TRUE)
//                .handler(new ChannelInitializer<SocketChannel>() {
//                    @Override
//                    protected void initChannel(SocketChannel ch) throws Exception {
//                        ChannelPipeline pipeline = ch.pipeline();
//                        pipeline.addLast(new ProtostuffDecoder(factoryManager));
//                        pipeline.addLast(new ProtostuffEncoder(factoryManager));
//                        pipeline.addLast(new SimpleChannelInboundHandler<Message>() {
//                            @Override
//                            protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
//                                log.debug("{}", msg);
//                            }
//                        });
//                    }
//                })
//                .connect(new InetSocketAddress("127.0.0.1", 6666))
//                .sync();
//            Channel channel = channelFuture.channel();
//            IntStream.range(0, 10000)
//                .forEach(index -> channel.writeAndFlush(SetCommand.builder().id(UUID.randomUUID().toString())
//                    .key("WANGHAO" + index)
//                    .value(String.valueOf(index))
//                    .build()));
//            IntStream.range(0, 1000).forEach(index -> channel
//                .writeAndFlush(GetCommand.builder().id(UUID.randomUUID().toString()).key("WANGHAO" + index).build()));
//
//        } catch (Exception e) {
//        }
//    }


}
