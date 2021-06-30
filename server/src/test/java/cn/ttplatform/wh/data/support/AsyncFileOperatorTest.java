package cn.ttplatform.wh.data.support;

import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.support.DirectByteBufferPool;
import cn.ttplatform.wh.support.Pool;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;

/**
 * @author Wang Hao
 * @date 2021/6/29 14:14
 */
@Slf4j
public class AsyncFileOperatorTest {

    AsyncFileOperator fileOperator;
    Pool<ByteBuffer> bufferPool;

    @Before
    public void setUp() throws Exception {
        bufferPool = new DirectByteBufferPool(10, 1024 * 1024, 10 * 1024 * 1024);
        File file = File.createTempFile("AsyncFileOperatorTest-", ".txt");
        fileOperator = new AsyncFileOperator(new ServerProperties(), bufferPool, file, 8);
    }

    @After
    public void tearDown() {
        fileOperator.close();
    }

    @Test
    public void read() {
        int cap = 10 * 1024 * 1024;
        byte[] bytes = new byte[cap];
        fileOperator.appendBytes(bytes);
        Assert.assertEquals(cap + 8, fileOperator.getSize());
        long begin = System.nanoTime();
        ByteBuffer[] read = fileOperator.read(8);
        int count = 0;
        for (ByteBuffer byteBuffer : read) {
            count += byteBuffer.limit();
        }
        Assert.assertEquals(cap, count);
        log.info("read {} bytes cost {} ns.", count, System.nanoTime() - begin);
    }

    @Test
    public void appendBlock() {
        int cap = 1024 * 1024;
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(cap);
        long begin = System.nanoTime();
        fileOperator.appendBlock(byteBuffer);
        log.info("append {} bytes cost {} ns.", cap, System.nanoTime() - begin);
        Assert.assertEquals(cap + 8, fileOperator.getSize());
    }

    @Test
    public void appendBytes() {
        int cap = 1024 * 1024;
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(cap);
        long begin = System.nanoTime();
        fileOperator.appendBytes(byteBuffer);
        log.info("append {} bytes cost {} ns.", cap, System.nanoTime() - begin);
        Assert.assertEquals(cap + 8, fileOperator.getSize());
    }


    @Test
    public void appendInt() {
        long begin = System.nanoTime();
        fileOperator.appendInt(1);
        log.info("append {} bytes cost {} ns.", Integer.BYTES, System.nanoTime() - begin);
        Assert.assertEquals(Integer.BYTES + 8, fileOperator.getSize());
    }

    @Test
    public void appendLong() {
        long begin = System.nanoTime();
        fileOperator.appendLong(1L);
        log.info("append {} bytes cost {} ns.", Long.BYTES, System.nanoTime() - begin);
        Assert.assertEquals(Long.BYTES + 8, fileOperator.getSize());
    }

    @Test
    public void getInt() {
        appendInt();
        long begin = System.nanoTime();
        fileOperator.getInt(8L);
        log.info("read {} bytes cost {} ns.", Integer.BYTES, System.nanoTime() - begin);
    }

    @Test
    public void getLong() {
        appendLong();
        long begin = System.nanoTime();
        fileOperator.getLong(8L);
        log.info("read {} bytes cost {} ns.", Long.BYTES, System.nanoTime() - begin);
    }


    @Test
    public void removeAfter() {
        appendBytes();
        long begin = System.nanoTime();
        fileOperator.truncate(8L);
        log.info("removeAfter cost {} ns.", System.nanoTime() - begin);
        Assert.assertEquals(8, fileOperator.getSize());
    }
}