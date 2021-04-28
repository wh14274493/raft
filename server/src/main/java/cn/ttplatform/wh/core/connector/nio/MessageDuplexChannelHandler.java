package cn.ttplatform.wh.core.connector.nio;

import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.support.ChannelPool;
import cn.ttplatform.wh.support.Message;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 * @author : wang hao
 * @date :  2020/8/16 0:19
 **/
@Slf4j
public class MessageDuplexChannelHandler extends AbstractDuplexChannelHandler {

    public MessageDuplexChannelHandler(NodeContext context) {
        super(context);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        String remoteId = ((Message) msg).getSourceId();
        ChannelPool.cacheChannel(remoteId, ctx.channel());
        super.channelRead(ctx, msg);
    }
}
