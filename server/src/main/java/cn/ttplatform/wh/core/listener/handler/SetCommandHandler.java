package cn.ttplatform.wh.core.listener.handler;

import cn.ttplatform.wh.cmd.SetCommand;
import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.core.GlobalContext;
import cn.ttplatform.wh.core.data.log.Log;
import cn.ttplatform.wh.core.support.AbstractDistributableHandler;
import cn.ttplatform.wh.support.Distributable;

/**
 * @author Wang Hao
 * @date 2021/2/21 15:56
 */
public class SetCommandHandler extends AbstractDistributableHandler {

    public SetCommandHandler(GlobalContext context) {
        super(context);
    }

    @Override
    public void doHandleInSingleMode(Distributable distributable) {
        SetCommand setCommand = (SetCommand) distributable;
        int currentTerm = context.getNode().getTerm();
        int index = context.pendingLog(Log.SET, context.getEntryFactory().getBytes(setCommand.getEntry()));
        context.addPendingCommand(index, setCommand);
        if (context.getLogContext().advanceCommitIndex(index, currentTerm)) {
            context.advanceLastApplied(index);
        }
    }

    @Override
    public void doHandleInClusterMode(Distributable distributable) {
        SetCommand setCommand = (SetCommand) distributable;
        int index = context.pendingLog(Log.SET, context.getEntryFactory().getBytes(setCommand.getEntry()));
        context.addPendingCommand(index, setCommand);
    }

    @Override
    public int getHandlerType() {
        return DistributableType.SET_COMMAND;
    }

}
