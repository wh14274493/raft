package cn.ttplatform.wh.core.data.snapshot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import cn.ttplatform.wh.core.data.tool.DirectByteBufferPool;
import cn.ttplatform.wh.support.ByteArrayPool;
import cn.ttplatform.wh.support.Pool;
import cn.ttplatform.wh.core.data.tool.PooledByteBuffer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Wang Hao
 * @date 2021/4/29 12:46
 */
@Slf4j
public class SnapshotFileTest {

    SnapshotFile snapshotFile;
    byte[] content;

    @Before
    public void setUp() {
        Pool<byte[]> byteArrayPool = new ByteArrayPool(10, 10 * 1024 * 1024);
        Pool<PooledByteBuffer> bufferPool = new DirectByteBufferPool(10, 10 * 1024 * 1024);
        String path = Objects.requireNonNull(SnapshotFileTest.class.getClassLoader().getResource("")).getPath();
        snapshotFile = new SnapshotFile(new File(path), bufferPool, byteArrayPool);
        int count = ThreadLocalRandom.current().nextInt(10000000);
        StringBuilder sb = new StringBuilder();
        IntStream.range(0, count).forEach(sb::append);
        content = sb.toString().getBytes(StandardCharsets.UTF_8);
        long begin = System.nanoTime();
//        snapshotFile.write(0, 0, content);
        log.info("write {} byte cost {} ns", content.length + SnapshotFile.HEADER_LENGTH, (System.nanoTime() - begin));
    }

    @After
    public void tearDown() {
        snapshotFile.clear();
        assertTrue(snapshotFile.isEmpty());
        snapshotFile.close();
    }

    @Test
    public void read() {
        long begin = System.nanoTime();
        int read = snapshotFile.read(0, content.length + SnapshotFile.HEADER_LENGTH).length;
        log.info("read {} bytes cost {} ns", content.length + SnapshotFile.HEADER_LENGTH, (System.nanoTime() - begin));
        assertEquals(content.length + SnapshotFile.HEADER_LENGTH, read);
    }

    @Test
    public void readAll() {
        long begin = System.nanoTime();
        int read = snapshotFile.readAll().limit();
        log.info("read {} bytes cost {} ns", content.length, (System.nanoTime() - begin));
        assertEquals(content.length, read);
    }

    @Test
    public void isEmpty() {
        assertFalse(snapshotFile.isEmpty());
    }

    @Test
    public void getSnapshotHeader() {
        SnapshotHeader snapshotHeader = snapshotFile.getSnapshotHeader();
        assertEquals(0, snapshotHeader.getLastIncludeIndex());
        assertEquals(0, snapshotHeader.getLastIncludeTerm());
        assertEquals(content.length, snapshotHeader.getContentLength());
    }
}