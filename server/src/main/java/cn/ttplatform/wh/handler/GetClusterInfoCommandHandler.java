package cn.ttplatform.wh.handler;

import cn.ttplatform.wh.GlobalContext;
import cn.ttplatform.wh.cmd.Command;
import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.support.AbstractDistributableHandler;
import cn.ttplatform.wh.support.Distributable;

/**
 * @author Wang Hao
 * @date 2021/4/28 10:56
 */
public class GetClusterInfoCommandHandler extends AbstractDistributableHandler {


    public GetClusterInfoCommandHandler(GlobalContext context) {
        super(context);
    }

    @Override
    public void doHandleInSingleMode(Distributable distributable) {
        context.replyGetClusterInfoResult(((Command) distributable).getId());
    }

    @Override
    public void doHandleInClusterMode(Distributable distributable) {
        context.replyGetClusterInfoResult(((Command) distributable).getId());
    }

    @Override
    public int getHandlerType() {
        return DistributableType.GET_CLUSTER_INFO_COMMAND;
    }
}
