package cn.ttplatform.wh.support;

/**
 * @author Wang Hao
 * @date 2021/2/18 11:26
 */
public interface Factory<T> {

    /**
     * use the byte array to create an Object
     *
     * @param content a serialized byte array
     * @return a deserialized object
     */
    T create(byte[] content);

    /**
     * use protostuff to serialize a message object
     *
     * @param obj obj
     * @return a serialized byte array
     */
    byte[] getBytes(T obj);
}
