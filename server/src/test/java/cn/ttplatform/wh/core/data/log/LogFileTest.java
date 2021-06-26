package cn.ttplatform.wh.core.data.log;

import static org.junit.Assert.assertEquals;

import cn.ttplatform.wh.data.log.Log;
import cn.ttplatform.wh.data.log.LogFactory;
import cn.ttplatform.wh.data.log.LogFile;
import cn.ttplatform.wh.data.tool.DirectByteBufferPool;
import cn.ttplatform.wh.support.Pool;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Wang Hao
 * @date 2021/4/29 12:31
 */
@Slf4j
public class LogFileTest {

    LogFile logFile;

    @Before
    public void setUp() throws IOException {
        Pool<ByteBuffer> bufferPool = new DirectByteBufferPool(10, 10 * 1024 * 1024);
        String property = System.getProperty("user.home");
        File file = File.createTempFile(property,"log.data");
        logFile = new LogFile(file, bufferPool);
    }

    @After
    public void tearDown() {
        logFile.removeAfter(0L);
        assertEquals(0, logFile.size());
    }

    @Test
    public void append() {
        byte[] content = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        long begin = System.nanoTime();
        logFile.append(LogFactory.createEntry(1, 1, 1, content, content.length));
        log.info("append 1 entry indices cost {} ns", (System.nanoTime() - begin));
    }

    @Test
    public void testAppend() {
        byte[] content = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        int capacity = ThreadLocalRandom.current().nextInt(1000000);
        List<Log> logEntries = new ArrayList<>(capacity);
        IntStream.range(0, capacity)
            .forEach(index -> logEntries.add(LogFactory.createEntry(1, 1, index + 1, content, content.length)));
        long begin = System.nanoTime();
        long[] append = logFile.append(logEntries);
        log.info("append {} entry indices cost {} ns", capacity, (System.nanoTime() - begin));
        assertEquals(capacity, append.length);
    }

    @Test
    public void getEntry() {
        byte[] content = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        logFile.append(LogFactory.createEntry(1, 1, 1, content, content.length));
        long begin = System.nanoTime();
        Log entry = logFile.getLog(0, logFile.size());
        log.info("load 1 entry index cost {} ns", (System.nanoTime() - begin));
        assertEquals(1, entry.getIndex());
    }

    @Test
    public void loadEntriesFromFile() {
        testAppend();
        long begin = System.nanoTime();
        List<Log> res = new ArrayList<>();
        logFile.loadLogsIntoList(0, logFile.size(), res);
        log.info("load {} LogEntry cost {} ns", res.size(), (System.nanoTime() - begin));
    }

}