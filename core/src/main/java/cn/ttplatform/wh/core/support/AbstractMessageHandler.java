package cn.ttplatform.wh.core.support;

import cn.ttplatform.wh.core.Node;
import cn.ttplatform.wh.core.connector.message.Message;

/**
 * @author Wang Hao
 * @date 2021/2/17 0:43
 */
public abstract class AbstractMessageHandler implements MessageHandler {

    protected Node node;

    protected AbstractMessageHandler(Node node) {
        this.node = node;
    }

    @Override
    public void handle(Message message) {
        node.getContext().getExecutor().execute(() -> doHandle(message));
    }

    /**
     * process a message
     * @param e message
     */
    public abstract void doHandle(Message e);
}
