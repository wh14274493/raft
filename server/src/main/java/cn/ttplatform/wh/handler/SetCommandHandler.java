package cn.ttplatform.wh.handler;

import cn.ttplatform.wh.cmd.SetCommand;
import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.GlobalContext;
import cn.ttplatform.wh.data.log.Log;
import cn.ttplatform.wh.support.AbstractDistributableHandler;
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
        if (context.getLogManager().advanceCommitIndex(index, currentTerm)) {
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
