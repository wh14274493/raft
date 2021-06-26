package cn.ttplatform.wh.data.log;

import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.data.FileConstant;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

@Slf4j
public class LogIndexBufferTest {

    LogIndexBuffer logIndexBuffer;

    @Before
    public void setUp() throws Exception {
        File file = File.createTempFile("temp-", "logIndexBuffer");
        logIndexBuffer = new LogIndexBuffer(file, new ServerProperties());
    }

    @After
    public void tearDown() {
        logIndexBuffer.close();
    }

    @Test
    public void getLastLogMetaData() {
        logIndexBuffer.append(LogFactory.createEntry(1, 1, 1, new byte[0], 0), FileConstant.LOG_INDEX_FILE_HEADER_SIZE);
        LogIndex lastEntryIndex = logIndexBuffer.getLastLogMetaData();
        assertNotNull(lastEntryIndex);
    }

    @Test
    public void getLogMetaData() {
        logIndexBuffer.append(LogFactory.createEntry(1, 1, 1, new byte[0], 0), FileConstant.LOG_INDEX_FILE_HEADER_SIZE);
        LogIndex entryMetaData = logIndexBuffer.getLogMetaData(1);
        assertEquals(FileConstant.LOG_INDEX_FILE_HEADER_SIZE, entryMetaData.getOffset());
        entryMetaData = logIndexBuffer.getLogMetaData(2);
        assertNull(entryMetaData);
    }

    @Test
    public void getEntryOffset() {
        logIndexBuffer.append(LogFactory.createEntry(1, 1, 1, new byte[0], 0), FileConstant.LOG_INDEX_FILE_HEADER_SIZE);
        long entryOffset = logIndexBuffer.getLogOffset(1);
        assertEquals(FileConstant.LOG_INDEX_FILE_HEADER_SIZE, entryOffset);
        entryOffset = logIndexBuffer.getLogOffset(2);
        assertEquals(-1, entryOffset);
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
        logIndexBuffer.append(logEntries, offsets);
        log.info("append {} log indices cost {} ns", capacity, (System.nanoTime() - begin));
    }

    @Test
    public void removeAfter() {
        testAppend();
        logIndexBuffer.removeAfter(1);
        Assert.assertEquals(logIndexBuffer.getMinIndex(), logIndexBuffer.getMaxIndex());
    }

}