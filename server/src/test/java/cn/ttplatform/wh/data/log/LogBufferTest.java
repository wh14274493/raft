package cn.ttplatform.wh.data.log;

import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.data.FileConstant;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

@Slf4j
public class LogBufferTest {

    LogBuffer logBuffer;

    @Before
    public void setUp() throws Exception {
        File file = File.createTempFile("temp-", "logBuffer");
        this.logBuffer = new LogBuffer(file, new ServerProperties());
    }

    @After
    public void tearDown() {
        logBuffer.close();
    }

    @Test
    public void append() {
        byte[] content = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        long begin = System.nanoTime();
        logBuffer.append(LogFactory.createEntry(1, 1, 1, content, content.length));
        log.info("append 1 log cost {} ns", System.nanoTime() - begin);
    }

    @Test
    public void testAppend() {
        byte[] content = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        int capacity = ThreadLocalRandom.current().nextInt(1000000);
        List<Log> logEntries = new ArrayList<>(capacity);
        IntStream.range(0, capacity)
                .forEach(index -> logEntries.add(LogFactory.createEntry(1, 1, index + 1, content, content.length)));
        long begin = System.nanoTime();
        long[] append = logBuffer.append(logEntries);
        log.info("append {} logs cost {} ns", capacity, System.nanoTime() - begin);
        assertEquals(capacity, append.length);
    }

    @Test
    public void getLog() {
        byte[] content = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        logBuffer.append(LogFactory.createEntry(1, 1, 1, content, content.length));
        long begin = System.nanoTime();
        Log entry = logBuffer.getLog(FileConstant.LOG_FILE_HEADER_SIZE, logBuffer.size());
        log.info("load 1 log cost {} ns", (System.nanoTime() - begin));
        assertEquals(1, entry.getIndex());
    }

    @Test
    public void loadLogsIntoList() {
        testAppend();
        long begin = System.nanoTime();
        List<Log> res = new ArrayList<>();
        logBuffer.loadLogsIntoList(FileConstant.LOG_FILE_HEADER_SIZE, logBuffer.size(), res);
        log.info("load {} logs cost {} ns", res.size(), (System.nanoTime() - begin));
    }

    @Test
    public void read() {
        testAppend();
        long begin = System.nanoTime();
        ByteBuffer[] read = logBuffer.read();
        log.info("load all cost {} ns", (System.nanoTime() - begin));
    }

    @Test
    public void transferTo() throws IOException {
        testAppend();
        File file = File.createTempFile("temp-", "logBuffer1");
        LogBuffer dest = new LogBuffer(file, new ServerProperties());
        long begin = System.nanoTime();
        logBuffer.transferTo(FileConstant.LOG_FILE_HEADER_SIZE,dest);
        log.info("transferTo all cost {} ns", (System.nanoTime() - begin));
    }

    @Test
    public void removeAfter() {
        testAppend();
        long begin = System.nanoTime();
        logBuffer.removeAfter(FileConstant.LOG_FILE_HEADER_SIZE);
        log.info("remove all cost {} ns", (System.nanoTime() - begin));
        Assert.assertEquals(FileConstant.LOG_FILE_HEADER_SIZE,logBuffer.size());
    }

    @Test
    public void close() {
    }

    @Test
    public void size() {
    }

    @Test
    public void isEmpty() {
    }
}