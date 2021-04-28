package cn.ttplatform.wh.core.listener.handler;

import cn.ttplatform.wh.cmd.SetCommand;
import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.log.Log;
import cn.ttplatform.wh.core.log.entry.LogEntry;
import cn.ttplatform.wh.core.log.entry.LogEntryFactory;
import cn.ttplatform.wh.core.support.AbstractDistributableHandler;
import cn.ttplatform.wh.support.Distributable;

/**
 * @author Wang Hao
 * @date 2021/2/21 15:56
 */
public class SetCommandHandler extends AbstractDistributableHandler {

    public SetCommandHandler(NodeContext context) {
        super(context);
    }

    @Override
    public void doHandle(Distributable distributable) {
        SetCommand cmd = (SetCommand) distributable;
        Log log = context.getLog();
        LogEntry logEntry = LogEntryFactory
            .createEntry(LogEntry.SET, context.getNode().getTerm(), 0, cmd.getCmd());
        cmd.setCmd(null);
        context.getStateMachine().addPendingCommand(logEntry.getIndex(), cmd);
        log.pendingEntry(logEntry);
    }

    @Override
    public int getHandlerType() {
        return DistributableType.SET_COMMAND;
    }
}
