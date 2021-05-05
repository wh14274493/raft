package cn.ttplatform.wh.support;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;
import java.util.List;

/**
 * @author Wang Hao
 * @date 2021/4/28 23:40
 */
public class DistributableCodec extends ByteToMessageCodec<Distributable> {

    private static final int FIXED_MESSAGE_HEADER_LENGTH = Integer.BYTES + Long.BYTES;

    private final DistributableFactoryManager factoryManager;
    private final Pool<byte[]> byteArrayPool;

    public DistributableCodec(DistributableFactoryManager factoryManager, Pool<byte[]> byteArrayPool) {
        this.factoryManager = factoryManager;
        this.byteArrayPool = byteArrayPool;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Distributable msg, ByteBuf out) {
        byte[] encode = encode(msg);
        out.writeInt(msg.getType());
        out.writeInt(encode.length);
        out.writeBytes(encode);
    }

    private byte[] encode(Distributable message) {
        DistributableFactory factory = factoryManager.getFactory(message.getType());
        return factory.getBytes(message);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < FIXED_MESSAGE_HEADER_LENGTH) {
            return;
        }
        in.markReaderIndex();
        int type = in.readInt();
        int contentLength = in.readInt();
        if (in.readableBytes() < contentLength) {
            in.resetReaderIndex();
            return;
        }
        byte[] content = byteArrayPool.allocate(contentLength);
        in.readBytes(content, 0, contentLength);
        DistributableFactory factory = factoryManager.getFactory(type);
        out.add(factory.create(content, contentLength));
    }
}
