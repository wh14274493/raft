package cn.ttplatform.wh.server.handler;

import cn.ttplatform.wh.cmd.SetCommand;
import cn.ttplatform.wh.support.Message;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.log.Log;
import cn.ttplatform.wh.core.log.entry.LogEntry;
import cn.ttplatform.wh.core.log.entry.LogEntryFactory;
import cn.ttplatform.wh.core.support.AbstractMessageHandler;

/**
 * @author Wang Hao
 * @date 2021/2/21 15:56
 */
public class SetCommandHandler extends AbstractMessageHandler {

    public SetCommandHandler(NodeContext context) {
        super(context);
    }

    @Override
    public void doHandle(Message e) {
        SetCommand cmd = (SetCommand) e;
        Log log = context.getLog();
        LogEntry logEntry = LogEntryFactory
            .createEntry(LogEntry.SET, context.getNode().getTerm(), 0, cmd.getCmd());
        cmd.setCmd(null);
        context.getStateMachine().addPendingCommand(logEntry.getIndex(), cmd);
        log.pendingEntry(logEntry);
    }

}
