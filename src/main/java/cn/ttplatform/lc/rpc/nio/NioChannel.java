package cn.ttplatform.lc.rpc.nio;


import cn.ttplatform.lc.rpc.message.Message;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import lombok.extern.slf4j.Slf4j;

/**
 * @author : wang hao
 * @description : NioChannel
 * @date :  2020/8/15 23:38
 **/
@Slf4j
public class NioChannel {

    private Channel channel;

    public NioChannel(Channel channel) {
        this.channel = channel;
    }

    public void write(final Message message) {
        channel.writeAndFlush(message).addListener(new GenericFutureListener<Future<? super Void>>() {
            public void operationComplete(Future<? super Void> future) throws Exception {
                if (future.isSuccess()) {
                    log.debug("send message {} success", message);
                } else {
                    log.debug("send message {} failed", message);
                }
            }
        });
    }

    public boolean isOpen() {
        return channel.isOpen();
    }
}
