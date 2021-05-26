package cn.ttplatform.wh.demo;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/5/13 23:50
 */
@Slf4j
@Sharable
public class ClientDuplexChannelHandler extends ChannelDuplexHandler {

    AtomicInteger index = new AtomicInteger();

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        channel.eventLoop().scheduleAtFixedRate(channel::flush, 10, 400, TimeUnit.MILLISECONDS);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        log.info("channelInactive");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//        if (index == 1) {
//            log.info(System.nanoTime() + "");
//            log.info(msg.toString());
//        }
//        if (index % 10000 == 0) {
//            log.info(System.nanoTime() + "");
//            log.info(msg.toString());
//        }
//                            log.info("{}: {} ", index, msg.toString());
//        index++;
        int i = index.incrementAndGet();
        if (i % 10000 == 0) {
            log.info("time = {}, count = {}, msg = {}", System.nanoTime(), i, msg);
        }

//        log.info(msg.toString());
    }

}
