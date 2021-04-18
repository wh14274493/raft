package cn.ttplatform.wh.core.connector.nio;

import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.domain.message.NodeIdMessage;
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

    MessageOutboundHandler(NodeContext context) {
        super(context);
        this.nodeId = context.node().getSelfId();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        Channel channel = ctx.channel();
        channel.writeAndFlush(NodeIdMessage.builder().sourceId(nodeId).build());
    }
}
