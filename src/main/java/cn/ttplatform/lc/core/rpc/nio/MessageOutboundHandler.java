package cn.ttplatform.lc.core.rpc.nio;

import cn.ttplatform.lc.core.rpc.message.MessageContext;
import cn.ttplatform.lc.core.rpc.message.domain.NodeIdMessage;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 * @author : wang hao
 * @date :  2020/8/16 0:21
 **/
@Slf4j
public class MessageOutboundHandler extends AbstractDuplexChannelHandler {

    String nodeId;

    MessageOutboundHandler(MessageContext context) {
        super(context);
        this.nodeId = context.getNode().getSelfId();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        Channel channel = ctx.channel();
        channel.writeAndFlush(NodeIdMessage.builder().sourceId(nodeId).build());

    }
}