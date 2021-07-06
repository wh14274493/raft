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
        File metaFile = File.createTempFile("AsyncLogMetaFile-", ".txt");
        fileOperator = new AsyncFileOperator(new ServerProperties(), bufferPool, file);
    }

    @After
    public void tearDown() {
        fileOperator.close();
    }

    @Test
    public void readBytes() {
        int cap = 10 * 1024 * 1024;
        byte[] bytes = new byte[cap];
        fileOperator.appendBytes(0, bytes);
//        Assert.assertEquals(cap, logFileMetadataRegion.getFileSize());
        long begin = System.nanoTime();
        ByteBuffer[] read = fileOperator.readBytes(0, cap);
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
        fileOperator.appendBlock(0, byteBuffer);
        log.info("append {} bytes cost {} ns.", cap, System.nanoTime() - begin);
//        Assert.assertEquals(cap, logFileMetadataRegion.getFileSize());
    }

    @Test
    public void appendBytes() {
        int cap = 1024 * 1024;
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(cap);
        long begin = System.nanoTime();
        fileOperator.appendBytes(0, byteBuffer);
        log.info("append {} bytes cost {} ns.", cap, System.nanoTime() - begin);
//        Assert.assertEquals(cap, logFileMetadataRegion.getFileSize());
    }


    @Test
    public void appendInt() {
        long begin = System.nanoTime();
        fileOperator.appendInt(0, 1);
        log.info("append {} bytes cost {} ns.", Integer.BYTES, System.nanoTime() - begin);
//        Assert.assertEquals(Integer.BYTES, logFileMetadataRegion.getFileSize());
    }

    @Test
    public void appendLong() {
        long begin = System.nanoTime();
        fileOperator.appendLong(0, 1L);
        log.info("append {} bytes cost {} ns.", Long.BYTES, System.nanoTime() - begin);
//        Assert.assertEquals(Long.BYTES, logFileMetadataRegion.getFileSize());
    }

    @Test
    public void getInt() {
        appendInt();
        long begin = System.nanoTime();
        fileOperator.getInt(0);
        log.info("read {} bytes cost {} ns.", Integer.BYTES, System.nanoTime() - begin);
    }

    @Test
    public void getLong() {
        appendLong();
        long begin = System.nanoTime();
        fileOperator.getLong(0);
        log.info("read {} bytes cost {} ns.", Long.BYTES, System.nanoTime() - begin);
    }

    @Test
    public void get() {
        appendBytes();
        int count = 10;
        byte[] bytes = new byte[count];
        bytes[bytes.length - 1] = 1;
        long begin = System.nanoTime();
        fileOperator.get(5, bytes);
        log.info("get {} bytes cost {} ns.", count, System.nanoTime() - begin);
        Assert.assertEquals(0, bytes[bytes.length - 1]);
    }

    @Test
    public void removeAfter() {
        appendBytes();
        long begin = System.nanoTime();
        fileOperator.truncate(0, 1024 * 1024);
        log.info("removeAfter cost {} ns.", System.nanoTime() - begin);
//        Assert.assertEquals(0, logFileMetadataRegion.getFileSize());
    }
}