package cn.ttplatform.wh.core.common;

import cn.ttplatform.wh.domain.message.Message;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import lombok.extern.slf4j.Slf4j;

/**
 * @author : wang hao
 * @date :  2020/8/16 0:14
 **/
@Slf4j
public class ProtostuffEncoder extends MessageToByteEncoder<Message> {

    private final MessageFactoryManager factoryManager;

    public ProtostuffEncoder(MessageFactoryManager factoryManager) {
        this.factoryManager = factoryManager;
    }

    /**
     * Encode a message into a {@link ByteBuf}. This method will be called for each written message that can be handled
     * by this encoder.
     *
     * @param ctx the {@link ChannelHandlerContext} which this {@link MessageToByteEncoder} belongs to
     * @param msg the message to encode
     * @param out the {@link ByteBuf} into which the encoded message will be written
     */
    @Override
    protected void encode(ChannelHandlerContext ctx, Message msg, ByteBuf out) {
        byte[] encode = encode(msg);
        out.writeInt(msg.getType());
        out.writeLong(encode.length);
        out.writeBytes(encode);
    }

    private byte[] encode(Message message) {
        MessageFactory messageFactory = factoryManager.getFactory(message.getType());
        return messageFactory.getBytes(message);
    }
}
