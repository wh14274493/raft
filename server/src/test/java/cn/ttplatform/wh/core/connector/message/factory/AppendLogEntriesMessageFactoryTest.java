package cn.ttplatform.wh.core.connector.message.factory;

import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.core.connector.message.AppendLogEntriesMessage;
import cn.ttplatform.wh.support.FixedSizeLinkedBufferPool;
import cn.ttplatform.wh.support.Pool;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.protostuff.LinkedBuffer;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Wang Hao
 * @date 2021/5/11 10:59
 */
@Slf4j
public class AppendLogEntriesMessageFactoryTest {

    AppendLogEntriesMessageFactory factory;

    @Before
    public void setUp() throws Exception {
        Pool<LinkedBuffer> pool = new FixedSizeLinkedBufferPool(10);
        factory = new AppendLogEntriesMessageFactory(pool);
    }

    @Test
    public void getFactoryType() {
        Assert.assertEquals(DistributableType.APPEND_LOG_ENTRIES, factory.getFactoryType());
    }

    @Test
    public void create() {
        AppendLogEntriesMessage appendLogEntriesMessage = AppendLogEntriesMessage.builder().matched(true).preLogTerm(1)
            .preLogIndex(1)
            .leaderCommitIndex(1).sourceId("A")
            .leaderId("A").term(1).logEntries(Collections.emptyList()).build();
        byte[] bytes = factory.getBytes(appendLogEntriesMessage);
        long begin = System.nanoTime();
        IntStream.range(0, 10000).forEach(index -> factory.create(bytes, bytes.length));
        log.info("deserialize 10000 times cost {} ns.", System.nanoTime() - begin);
    }

    @Test
    public void testCreate() {
        AppendLogEntriesMessage appendLogEntriesMessage = AppendLogEntriesMessage.builder().matched(true).preLogTerm(1)
            .preLogIndex(1)
            .leaderCommitIndex(1).sourceId("A")
            .leaderId("A").term(1).logEntries(Collections.emptyList()).build();
        byte[] bytes = factory.getBytes(appendLogEntriesMessage);
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
        AppendLogEntriesMessage appendLogEntriesMessage = AppendLogEntriesMessage.builder().matched(true).preLogTerm(1)
            .preLogIndex(1)
            .leaderCommitIndex(1).sourceId("A")
            .leaderId("A").term(1).logEntries(Collections.emptyList()).build();
        long begin = System.nanoTime();
        IntStream.range(0, 10000).forEach(index -> factory.getBytes(appendLogEntriesMessage));
        log.info("serialize 10000 times cost {} ns.", System.nanoTime() - begin);
    }

    @Test
    public void testGetBytes() {
        AppendLogEntriesMessage appendLogEntriesMessage = AppendLogEntriesMessage.builder().matched(true).preLogTerm(1)
            .preLogIndex(1)
            .leaderCommitIndex(1).sourceId("A")
            .leaderId("A").term(1).logEntries(Collections.emptyList()).build();
        UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(true);
        ByteBuf byteBuf = allocator.directBuffer();
        long begin = System.nanoTime();
        IntStream.range(0, 10000).forEach(index -> {
            factory.getBytes(appendLogEntriesMessage,  byteBuf);
            byteBuf.clear();
        });
        log.info("serialize 10000 times cost {} ns.", System.nanoTime() - begin);

    }
}