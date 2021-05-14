package cn.ttplatform.wh.core.log.snapshot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import cn.ttplatform.wh.core.log.tool.DirectByteBufferPool;
import cn.ttplatform.wh.support.ByteArrayPool;
import cn.ttplatform.wh.support.Pool;
import cn.ttplatform.wh.core.log.tool.PooledByteBuffer;
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
public class FileSnapshotTest {

    FileSnapshot fileSnapshot;
    byte[] content;

    @Before
    public void setUp() {
        Pool<byte[]> byteArrayPool = new ByteArrayPool(10, 10 * 1024 * 1024);
        Pool<PooledByteBuffer> bufferPool = new DirectByteBufferPool(10, 10 * 1024 * 1024);
        String path = Objects.requireNonNull(FileSnapshotTest.class.getClassLoader().getResource("")).getPath();
        fileSnapshot = new FileSnapshot(new File(path), bufferPool, byteArrayPool, true);
        int count = ThreadLocalRandom.current().nextInt(10000000);
        StringBuilder sb = new StringBuilder();
        IntStream.range(0, count).forEach(sb::append);
        content = sb.toString().getBytes(StandardCharsets.UTF_8);
        long begin = System.nanoTime();
        fileSnapshot.write(0, 0, content);
        log.info("write {} byte cost {} ns", content.length + FileSnapshot.HEADER_LENGTH, (System.nanoTime() - begin));
    }

    @After
    public void tearDown() {
        fileSnapshot.clear();
        assertTrue(fileSnapshot.isEmpty());
        fileSnapshot.close();
    }

    @Test
    public void read() {
        long begin = System.nanoTime();
        int read = fileSnapshot.read(0, content.length + FileSnapshot.HEADER_LENGTH).length;
        log.info("read {} bytes cost {} ns", content.length + FileSnapshot.HEADER_LENGTH, (System.nanoTime() - begin));
        assertEquals(content.length + FileSnapshot.HEADER_LENGTH, read);
    }

    @Test
    public void readAll() {
        long begin = System.nanoTime();
        int read = fileSnapshot.readAll().limit();
        log.info("read {} bytes cost {} ns", content.length, (System.nanoTime() - begin));
        assertEquals(content.length, read);
    }

    @Test
    public void append() {
        long begin = System.nanoTime();
        fileSnapshot.append(content);
        log.info("append {} bytes cost {} ns", content.length, (System.nanoTime() - begin));
        int read = fileSnapshot.read(FileSnapshot.HEADER_LENGTH, content.length * 2).length;
        log.info("read {} bytes cost {} ns", read, (System.nanoTime() - begin));
        assertEquals(content.length * 2, read);
    }

    @Test
    public void isEmpty() {
        assertFalse(fileSnapshot.isEmpty());
    }

    @Test
    public void getSnapshotHeader() {
        SnapshotHeader snapshotHeader = fileSnapshot.getSnapshotHeader();
        assertEquals(0, snapshotHeader.getLastIncludeIndex());
        assertEquals(0, snapshotHeader.getLastIncludeTerm());
        assertEquals(content.length, snapshotHeader.getContentLength());
    }
}