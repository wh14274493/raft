package cn.ttplatform.wh.core.group;

import cn.ttplatform.wh.support.Pool;
import cn.ttplatform.wh.support.Factory;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import java.nio.ByteBuffer;

/**
 * @author Wang Hao
 * @date 2021/4/28 10:29
 */
public class NewConfigFactory implements Factory<NewConfig> {

    private final Pool<LinkedBuffer> pool;
    private final Schema<NewConfig> schema = RuntimeSchema.getSchema(NewConfig.class);

    public NewConfigFactory(Pool<LinkedBuffer> pool) {
        this.pool = pool;
    }

    @Override
    public NewConfig create(byte[] content, int length) {
        NewConfig newConfig = new NewConfig();
        ProtostuffIOUtil.mergeFrom(content, 0, length, newConfig, schema);
        return newConfig;
    }

    @Override
    public NewConfig create(ByteBuffer byteBuffer, int contentLength) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getBytes(NewConfig newConfig) {
        return ProtostuffIOUtil.toByteArray(newConfig, schema, pool.allocate());
    }
}
