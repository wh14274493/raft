package cn.ttplatform.wh.group;

import cn.ttplatform.wh.GlobalContext;
import cn.ttplatform.wh.support.AbstractChannelInitializer;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelPipeline;

/**
 * @author : Wang Hao
 * @date :  2020/8/16 18:22
 **/
@Sharable
public class CoreChannelInitializer extends AbstractChannelInitializer {


    public CoreChannelInitializer(GlobalContext context) {
        super(context);
    }

    @Override
    protected void custom(ChannelPipeline pipeline) {
        pipeline.addLast(new CoreDuplexChannelHandler(context));
    }
}
