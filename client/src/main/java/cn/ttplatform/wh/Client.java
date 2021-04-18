package cn.ttplatform.wh;

import cn.ttplatform.wh.constant.MessageType;
import cn.ttplatform.wh.core.common.BufferPool;
import cn.ttplatform.wh.core.common.FixedSizeLinkedBufferPool;
import cn.ttplatform.wh.core.common.MessageFactoryManager;
import cn.ttplatform.wh.core.common.ProtostuffDecoder;
import cn.ttplatform.wh.core.common.ProtostuffEncoder;
import cn.ttplatform.wh.domain.cmd.GetCommand;
import cn.ttplatform.wh.domain.cmd.SetCommand;
import cn.ttplatform.wh.domain.cmd.factory.GetCommandFactory;
import cn.ttplatform.wh.domain.cmd.factory.GetResponseCommandFactory;
import cn.ttplatform.wh.domain.cmd.factory.SetCommandFactory;
import cn.ttplatform.wh.domain.cmd.factory.SetResponseCommandFactory;
import cn.ttplatform.wh.domain.message.Message;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.protostuff.LinkedBuffer;
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/4/12 22:50
 */
@Slf4j
public class Client {

    public static void main(String[] args) {
        BufferPool<LinkedBuffer> bufferPool = new FixedSizeLinkedBufferPool(3);
        MessageFactoryManager factoryManager = new MessageFactoryManager();
        factoryManager.register(MessageType.SET_COMMAND, new SetCommandFactory(bufferPool));
        factoryManager.register(MessageType.GET_COMMAND, new GetCommandFactory(bufferPool));
        factoryManager.register(MessageType.SET_COMMAND_RESPONSE, new SetResponseCommandFactory(bufferPool));
        factoryManager.register(MessageType.GET_COMMAND_RESPONSE, new GetResponseCommandFactory(bufferPool));
        Bootstrap bootstrap = new Bootstrap();
        try {
            ChannelFuture channelFuture = bootstrap.group(new NioEventLoopGroup(1))
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, Boolean.TRUE)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new ProtostuffDecoder(factoryManager));
                        pipeline.addLast(new ProtostuffEncoder(factoryManager));
                        pipeline.addLast(new SimpleChannelInboundHandler<Message>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
                                log.debug("{}", msg);
                            }
                        });
                    }
                })
                .connect(new InetSocketAddress("127.0.0.1", 6666))
                .sync();
            Channel channel = channelFuture.channel();
            IntStream.range(0, 1000)
                .forEach(index -> channel.writeAndFlush(SetCommand.builder().id(UUID.randomUUID().toString())
                    .key("WANGHAO" + index)
                    .value(String.valueOf(index))
                    .build()));
            IntStream.range(0, 1000).forEach(index -> channel
                .writeAndFlush(GetCommand.builder().id(UUID.randomUUID().toString()).key("WANGHAO" + index).build()));

        } catch (Exception e) {
        }
    }
}
