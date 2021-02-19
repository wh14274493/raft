package cn.ttplatform.lc.core.rpc.message.handler;

import cn.ttplatform.lc.core.Node;
import cn.ttplatform.lc.core.rpc.message.domain.Message;

/**
 * @author Wang Hao
 * @date 2021/2/17 0:43
 */
public abstract class AbstractMessageHandler implements MessageHandler {

    Node node;

    AbstractMessageHandler(Node node) {
        this.node = node;
    }

    @Override
    public void handle(Message message) {
        node.getContext().executor().execute(() -> doHandle(message));
    }

    public abstract void doHandle(Message e);
}
