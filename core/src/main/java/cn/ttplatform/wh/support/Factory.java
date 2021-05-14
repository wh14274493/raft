package cn.ttplatform.wh.support;

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;

/**
 * @author Wang Hao
 * @date 2021/2/18 11:26
 */
public interface Factory<T> {

    /**
     * use the byte array to create an Object
     *
     * @param content       source byte array
     * @param contentLength valid bytes
     * @return a deserialized object
     */
    T create(byte[] content, int contentLength);

    /**
     * use the byteBuffer to create an Object
     *
     * @param byteBuffer    source buffer
     * @param contentLength valid bytes
     * @return a deserialized object
     */
    T create(ByteBuffer byteBuffer, int contentLength);

    /**
     * use protostuff to serialize a message object
     *
     * @param obj obj
     * @return a serialized byte array
     */
    byte[] getBytes(T obj);

    /**
     * use protostuff to serialize a message object into ByteBuf
     *
     * @param obj        target obj
     * @param byteBuffer dest buffer
     */
    void getBytes(T obj, ByteBuf byteBuffer);
}
