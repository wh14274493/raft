package cn.ttplatform.wh.core.support;

import cn.ttplatform.wh.common.Message;
import cn.ttplatform.wh.support.Factory;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;

/**
 * @author : wang hao
 * @date :  2020/8/16 0:17
 **/
public class ProtostuffDecoder extends ByteToMessageDecoder {

    private static final int FIXED_MESSAGE_HEADER_LENGTH = Integer.BYTES + Long.BYTES;

    private final MessageFactoryManager factoryManager;

    public ProtostuffDecoder(MessageFactoryManager factoryManager) {
        this.factoryManager = factoryManager;
    }

    /**
     * Decode the from one {@link ByteBuf} to an other. This method will be called till either the input {@link ByteBuf}
     * has nothing to read when return from this method or till nothing was read from the input {@link ByteBuf}.
     *
     * @param ctx the {@link ChannelHandlerContext} which this {@link ByteToMessageDecoder} belongs to
     * @param in  the {@link ByteBuf} from which to read data
     * @param out the {@link List} to which decoded messages should be added
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < FIXED_MESSAGE_HEADER_LENGTH) {
            return;
        }
        in.markReaderIndex();
        int type = in.readInt();
        long contentLength = in.readLong();
        if (in.readableBytes() < contentLength) {
            in.resetReaderIndex();
            return;
        }
        byte[] content = new byte[(int) contentLength];
        in.readBytes(content);
        @SuppressWarnings("unchecked")
        Factory<Message> factory = factoryManager.getFactory(type);
        out.add(factory.create(content));
    }
}
