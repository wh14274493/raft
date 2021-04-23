package cn.ttplatform.wh.core.support;

import cn.ttplatform.wh.cmd.Message;
import cn.ttplatform.wh.core.NodeContext;

/**
 * @author Wang Hao
 * @date 2021/2/17 0:43
 */
public abstract class AbstractMessageHandler implements MessageHandler {

    protected NodeContext context;

    protected AbstractMessageHandler(NodeContext context) {
        this.context = context;
    }

    @Override
    public void handle(Message message) {
        context.getExecutor().execute(() -> doHandle(message));
    }

    /**
     * process a message
     * @param message message
     */
    public abstract void doHandle(Message message);
}
