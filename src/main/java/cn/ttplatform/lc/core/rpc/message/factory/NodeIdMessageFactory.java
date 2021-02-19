package cn.ttplatform.lc.core.rpc.message.factory;

import cn.ttplatform.lc.core.rpc.message.domain.Message;
import cn.ttplatform.lc.core.rpc.message.domain.NodeIdMessage;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/2/18 16:15
 */
public class NodeIdMessageFactory implements MessageFactory {

    private final Schema<NodeIdMessage> schema = RuntimeSchema.getSchema(NodeIdMessage.class);

    @Override
    public Message create(byte[] content) {
        NodeIdMessage message = new NodeIdMessage();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(LinkedBuffer buffer, Message message) {
        return ProtostuffIOUtil.toByteArray((NodeIdMessage) message, schema, buffer);
    }
}
