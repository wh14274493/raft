package cn.ttplatform.wh.core.data.log;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import cn.ttplatform.wh.core.data.tool.DirectByteBufferPool;
import cn.ttplatform.wh.core.data.tool.PooledByteBuffer;
import cn.ttplatform.wh.support.ByteArrayPool;
import cn.ttplatform.wh.support.Pool;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Wang Hao
 * @date 2021/4/29 11:56
 */
@Slf4j
public class LogFileIndexTest {

    LogIndexFile logIndexFile;
    String path;

    @Before
    public void setUp() throws IOException {
        Pool<PooledByteBuffer> bufferPool = new DirectByteBufferPool(10, 10 * 1024 * 1024);
        Pool<byte[]> byteArrAyPool = new ByteArrayPool(10, 10 * 1024 * 1024);
        path = Objects.requireNonNull(LogFileIndexTest.class.getClassLoader().getResource("")).getPath();
        File file = new File(path, "log.index");
        Files.deleteIfExists(file.toPath());
        logIndexFile = new LogIndexFile(file, bufferPool, 0);
    }

    @After
    public void tearDown() {
        logIndexFile.removeAfter(0);
        logIndexFile.close();
    }

    @Test
    public void getLastEntryIndex() {
        logIndexFile.append(LogFactory.createEntry(1, 1, 1, new byte[0], 0), 0L);
        LogIndex lastEntryIndex = logIndexFile.getLastLogMetaData();
        assertNotNull(lastEntryIndex);
    }

    @Test
    public void getEntryOffset() {
        logIndexFile.append(LogFactory.createEntry(1, 1, 1, new byte[0], 0), 0L);
        long entryOffset = logIndexFile.getEntryOffset(1);
        assertEquals(0, entryOffset);
        entryOffset = logIndexFile.getEntryOffset(2);
        assertEquals(-1, entryOffset);
    }

    @Test
    public void getEntryMetaData() {
        logIndexFile.append(LogFactory.createEntry(1, 1, 1, new byte[0], 0), 0L);
        LogIndex entryMetaData = logIndexFile.getLogMetaData(1);
        assertEquals(0, entryMetaData.getOffset());
        entryMetaData = logIndexFile.getLogMetaData(2);
        assertNull(entryMetaData);
    }

    @Test
    public void testAppend() {
        int capacity = ThreadLocalRandom.current().nextInt(1000000);
        List<Log> logEntries = new ArrayList<>(capacity);
        long[] offsets = new long[capacity];
        IntStream.range(0, capacity).forEach(index -> {
            logEntries.add(LogFactory.createEntry(1, 1, index + 1, new byte[0], 0));
            offsets[index] = index;
        });
        long begin = System.nanoTime();
        logIndexFile.append(logEntries, offsets);
        log.debug("append {} entry indices cost {} ns", capacity, (System.nanoTime() - begin));
    }

    @Test
    public void removeAfter() {
        testAppend();
        logIndexFile.removeAfter(1);
        Assert.assertEquals(logIndexFile.getMinIndex(), logIndexFile.getMaxIndex());
    }

    @Test
    public void transferTo() throws IOException {
        testAppend();
        File file = new File(path, "log-temp.index");
        Files.deleteIfExists(file.toPath());
        long begin = System.nanoTime();
        logIndexFile.transferTo(20, file.toPath());
        log.debug("transfer indices cost {} ns", (System.nanoTime() - begin));
    }
}