package cn.ttplatform.wh.server.handler;

import cn.ttplatform.wh.common.Message;
import cn.ttplatform.wh.cmd.SetCommand;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.log.entry.LogEntry;
import cn.ttplatform.wh.core.log.entry.LogFactory;
import cn.ttplatform.wh.core.support.AbstractMessageHandler;

/**
 * @author Wang Hao
 * @date 2021/2/21 15:56
 */
public class SetCommandHandler extends AbstractMessageHandler {

    private final LogFactory factory = LogFactory.getInstance();

    public SetCommandHandler(NodeContext context) {
        super(context);
    }

    @Override
    public void doHandle(Message e) {
        SetCommand cmd = (SetCommand) e;
        LogEntry logEntry = factory
            .createEntry(LogEntry.OP_TYPE, context.getNode().getTerm(), context.getLog().getNextIndex(), cmd.getCmd());
        cmd.setCmd(null);
        context.getStateMachine().addPendingCommand(logEntry.getIndex(), cmd);
        context.getLog().pendingEntry(logEntry);
    }

}
