package cn.ttplatform.wh;

//import cn.ttplatform.wh.core.connector.message.Message;
//import cn.ttplatform.wh.core.support.BufferPool;
//import cn.ttplatform.wh.core.support.FixedSizeLinkedBufferPool;
//import cn.ttplatform.wh.core.support.MessageFactoryManager;
//import cn.ttplatform.wh.core.support.ProtostuffDecoder;
//import cn.ttplatform.wh.core.support.ProtostuffEncoder;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/4/12 22:50
 */
@Slf4j
public class Client {

    public static void main(String[] args) {
//        BufferPool<LinkedBuffer> bufferPool = new FixedSizeLinkedBufferPool(3);
//        MessageFactoryManager factoryManager = new MessageFactoryManager();
//        factoryManager.register(MessageType.SET_COMMAND, new SetCommandMessageFactory(bufferPool));
//        factoryManager.register(MessageType.GET_COMMAND, new GetCommandMessageFactory(bufferPool));
//        factoryManager.register(MessageType.SET_COMMAND_RESPONSE, new SetResponseCommandMessageFactory(bufferPool));
//        factoryManager.register(MessageType.GET_COMMAND_RESPONSE, new GetResponseCommandMessageFactory(bufferPool));
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
//                            int i = 1;
//
//                            @Override
//                            protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
//                                if (i == 1) {
//                                    log.debug("RECEIVE 1 AT {}", System.currentTimeMillis());
//                                }
//                                if (i == 10000) {
//                                    log.debug("RECEIVE 10000 AT {}", System.currentTimeMillis());
//                                }
//                                if (i == 9999) {
//                                    log.debug("RECEIVE 10000 AT {}", System.currentTimeMillis());
//                                }
//                                i++;
//                            }
//                        });
//                    }
//                })
//                .connect(new InetSocketAddress("127.0.0.1", 9999))
//                .sync();
//            Channel channel = channelFuture.channel();
//
//            String s = "";
//            while (s.length() < 256) {
//                s += UUID.randomUUID().toString();
//            }
//            s = s.substring(0,256);
//            log.debug(" START {}", System.currentTimeMillis());
//            String finalS = s;
//            String keyPrefix = UUID.randomUUID().toString();
//            IntStream.range(0, 10000).forEach(index -> channel.writeAndFlush(
//                SetCommand.builder().id(UUID.randomUUID().toString())
//                    .key(keyPrefix + index)
//                    .value(finalS)
//                    .build()));
////            IntStream.range(0, 1000).forEach(index -> channel
////                .writeAndFlush(GetCommand.builder().id(UUID.randomUUID().toString()).key("WANGHAO" + index).build()));
//            Thread.sleep(9999999);
//        } catch (Exception e) {
//        }
    }
}
