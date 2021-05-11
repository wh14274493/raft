package cn.ttplatform.wh.core.connector.message.factory;

import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.core.connector.message.RequestVoteResultMessage;
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
 * @date 2021/5/11 23:03
 */
@Slf4j
public class RequestVoteResultMessageFactoryTest {

    RequestVoteResultMessageFactory factory;

    @Before
    public void setUp() throws Exception {
        Pool<LinkedBuffer> pool = new FixedSizeLinkedBufferPool(10);
        factory = new RequestVoteResultMessageFactory(pool);
    }

    @Test
    public void getFactoryType() {
        Assert.assertEquals(DistributableType.REQUEST_VOTE_RESULT, factory.getFactoryType());
    }

    @Test
    public void create() {
        RequestVoteResultMessage message = RequestVoteResultMessage.builder()
            .term(0).sourceId("A").isVoted(true).build();
        byte[] bytes = factory.getBytes(message);
        long begin = System.nanoTime();
        IntStream.range(0, 10000).forEach(index -> factory.create(bytes, bytes.length));
        log.info("deserialize 10000 times cost {} ns.", System.nanoTime() - begin);
    }

    @Test
    public void testCreate() {
        RequestVoteResultMessage message = RequestVoteResultMessage.builder()
            .term(0).sourceId("A").isVoted(true).build();
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
        RequestVoteResultMessage message = RequestVoteResultMessage.builder()
            .term(0).sourceId("A").isVoted(true).build();
        long begin = System.nanoTime();
        IntStream.range(0, 10000).forEach(index -> factory.getBytes(message));
        log.info("serialize 10000 times cost {} ns.", System.nanoTime() - begin);
    }

    @Test
    public void testGetBytes() {
        RequestVoteResultMessage message = RequestVoteResultMessage.builder()
            .term(0).sourceId("A").isVoted(true).build();
        UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(true);
        ByteBuf byteBuf = allocator.directBuffer();
        long begin = System.nanoTime();
        IntStream.range(0, 10000).forEach(index -> {
            factory.getBytes(message, byteBuf);
            byteBuf.clear();
        });
        log.info("serialize 10000 times cost {} ns.", System.nanoTime() - begin);
    }
}