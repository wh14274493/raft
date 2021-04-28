package cn.ttplatform.wh.core.connector.message.factory;

import cn.ttplatform.wh.support.AbstractMessageFactory;
import cn.ttplatform.wh.support.BufferPool;
import cn.ttplatform.wh.support.Message;
import cn.ttplatform.wh.core.connector.message.NodeIdMessage;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/2/18 16:15
 */
public class NodeIdMessageFactory extends AbstractMessageFactory {

    private final Schema<NodeIdMessage> schema = RuntimeSchema.getSchema(NodeIdMessage.class);

    NodeIdMessageFactory(BufferPool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public Message create(byte[] content) {
        NodeIdMessage message = new NodeIdMessage();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(Message message, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((NodeIdMessage) message, schema, buffer);
    }
}
