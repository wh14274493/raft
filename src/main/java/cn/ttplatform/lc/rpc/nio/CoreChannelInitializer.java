package cn.ttplatform.lc.rpc.nio;

import cn.ttplatform.lc.event.EventHandler;
import cn.ttplatform.lc.node.Node;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author : Wang Hao
 * @date :  2020/8/16 18:22
 **/
public class CoreChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final EventHandler<Node> handler;
    private final Map<String, NioChannel> in;
    private Map<String, NioChannel> out;

    public CoreChannelInitializer(EventHandler<Node> handler, Map<String, NioChannel> in, Map<String, NioChannel> out) {
        this.handler = handler;
        this.in = in;
        this.out = out;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast(new CoreProtoBufDecoder());
        pipeline.addLast(new CoreProtoBufEncoder());
        pipeline.addLast(new MessageInboundHandler(handler, in));
        pipeline.addLast(new MessageOutboundHandler());
    }
}
