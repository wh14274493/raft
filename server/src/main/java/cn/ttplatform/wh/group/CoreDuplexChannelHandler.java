package cn.ttplatform.wh.group;

import cn.ttplatform.wh.GlobalContext;
import cn.ttplatform.wh.support.AbstractDuplexChannelHandler;
import cn.ttplatform.wh.support.Message;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 * @author : wang hao
 * @date :  2020/8/16 0:19
 **/
@Slf4j
@Sharable
public class CoreDuplexChannelHandler extends AbstractDuplexChannelHandler {

    public CoreDuplexChannelHandler(GlobalContext context) {
        super(context);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        Channel channel = ctx.channel();
        if (msg instanceof Message) {
            String sourceId = ((Message) msg).getSourceId();
            log.debug("receive a msg {} from {}.", msg, sourceId);
            channelPool.cacheChannel(sourceId, channel);
            distributor.distribute((Message) msg);
        } else {
            log.error("unknown message type, msg is {}", msg);
        }
    }
}