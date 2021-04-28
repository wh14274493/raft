package cn.ttplatform.wh.core.group;

import cn.ttplatform.wh.support.BufferPool;
import cn.ttplatform.wh.support.Factory;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/4/28 10:29
 */
public class NewConfigFactory implements Factory<NewConfig> {

    private final BufferPool<LinkedBuffer> pool;
    private final Schema<NewConfig> schema = RuntimeSchema.getSchema(NewConfig.class);

    public NewConfigFactory(BufferPool<LinkedBuffer> pool) {
        this.pool = pool;
    }

    @Override
    public NewConfig create(byte[] content) {
        NewConfig newConfig = new NewConfig();
        ProtostuffIOUtil.mergeFrom(content, newConfig, schema);
        return newConfig;
    }

    @Override
    public byte[] getBytes(NewConfig newConfig) {
        return ProtostuffIOUtil.toByteArray(newConfig, schema, pool.allocate());
    }
}
