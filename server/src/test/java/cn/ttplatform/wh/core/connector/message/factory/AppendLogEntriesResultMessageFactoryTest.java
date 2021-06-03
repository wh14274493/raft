package cn.ttplatform.wh.core.connector.message.factory;

import cn.ttplatform.wh.message.factory.AppendLogEntriesResultMessageFactory;
import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.message.AppendLogEntriesResultMessage;
import cn.ttplatform.wh.support.FixedSizeLinkedBufferPool;
import cn.ttplatform.wh.support.Pool;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.protostuff.LinkedBuffer;
import java.nio.ByteBuffer;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Wang Hao
 * @date 2021/5/11 11:08
 */
@Slf4j
public class AppendLogEntriesResultMessageFactoryTest {

    AppendLogEntriesResultMessageFactory factory;

    @Before
    public void setUp() throws Exception {
        Pool<LinkedBuffer> pool = new FixedSizeLinkedBufferPool(10);
        factory = new AppendLogEntriesResultMessageFactory(pool);
    }

    @Test
    public void getFactoryType() {
        Assert.assertEquals(DistributableType.APPEND_LOG_ENTRIES_RESULT, factory.getFactoryType());
    }

    @Test
    public void create() {
        AppendLogEntriesResultMessage message = AppendLogEntriesResultMessage.builder().lastLogIndex(1).sourceId("A")
            .success(true).term(1).build();
        byte[] bytes = factory.getBytes(message);
        long begin = System.nanoTime();
        IntStream.range(0, 10000).forEach(index -> factory.create(bytes, bytes.length));
        log.info("deserialize 10000 times cost {} ns.", System.nanoTime() - begin);
    }

    @Test
    public void testCreate() {
        AppendLogEntriesResultMessage message = AppendLogEntriesResultMessage.builder().lastLogIndex(1).sourceId("A")
            .success(true).term(1).build();
        byte[] bytes = factory.getBytes(message);
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(bytes.length);
        byteBuffer.put(bytes);
        byteBuffer.flip();
        long begin = System.nanoTime();
        IntStream.range(0, 10000).forEach(index -> {
            factory.create(byteBuffer, bytes.length);
            byteBuffer.position(0);
        });
        log.info("deserialize 10000 times cost {} ns.", System.nanoTime() - begin);
    }

    @Test
    public void getBytes() {
        AppendLogEntriesResultMessage message = AppendLogEntriesResultMessage.builder().lastLogIndex(1).sourceId("A")
            .success(true).term(1).build();
        long begin = System.nanoTime();
        IntStream.range(0, 10000).forEach(index -> factory.getBytes(message));
        log.info("serialize 10000 times cost {} ns.", System.nanoTime() - begin);
    }

    @Test
    public void testGetBytes() {
        AppendLogEntriesResultMessage message = AppendLogEntriesResultMessage.builder().lastLogIndex(1).sourceId("A")
            .success(true).term(1).build();
        UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(true);
        ByteBuf byteBuf = allocator.directBuffer();
        long begin = System.nanoTime();
        IntStream.range(0, 10000).forEach(index -> {
            factory.getBytes(message,  byteBuf);
            byteBuf.clear();
        });
        log.info("serialize 10000 times cost {} ns.", System.nanoTime() - begin);
    }
}