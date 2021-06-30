package cn.ttplatform.wh.core.data.log;

import cn.ttplatform.wh.data.FileConstant;
import cn.ttplatform.wh.data.log.Log;
import cn.ttplatform.wh.data.log.LogFactory;
import cn.ttplatform.wh.data.log.SyncLogFile;
import cn.ttplatform.wh.support.DirectByteBufferPool;
import cn.ttplatform.wh.support.Pool;
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
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

/**
 * @author Wang Hao
 * @date 2021/4/29 12:31
 */
@Slf4j
public class SyncLogFileTest {

    SyncLogFile syncLogFile;

    @Before
    public void setUp() throws IOException {
        Pool<ByteBuffer> bufferPool = new DirectByteBufferPool(10, 1024 * 1024, 10 * 1024 * 1024);
        File file = File.createTempFile("SyncLogFile-", ".txt");
        syncLogFile = new SyncLogFile(file, bufferPool);
    }

    @After
    public void tearDown() {
        syncLogFile.close();
    }

    @Test
    public void append() {
        byte[] content = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        long begin = System.nanoTime();
        syncLogFile.append(LogFactory.createEntry(1, 1, 1, content));
        log.info("append 1 log cost {} ns", (System.nanoTime() - begin));
        Assert.assertEquals(Log.HEADER_BYTES + content.length + FileConstant.LOG_FILE_HEADER_SIZE, syncLogFile.size());
    }

    @Test
    public void testAppend() {
        byte[] content = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        int capacity = 100000;
        List<Log> logEntries = new ArrayList<>(capacity);
        IntStream.range(0, capacity).forEach(index -> logEntries.add(LogFactory.createEntry(1, 1, index + 1, content)));
        long begin = System.nanoTime();
        long[] append = syncLogFile.append(logEntries);
        log.info("append {} logs cost {} ns", capacity, (System.nanoTime() - begin));
        assertEquals(capacity, append.length);
    }

    @Test
    public void getLog() {
        byte[] content = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        syncLogFile.append(LogFactory.createEntry(1, 1, 1, content));
        long begin = System.nanoTime();
        Log entry = syncLogFile.getLog(FileConstant.LOG_FILE_HEADER_SIZE, syncLogFile.size());
        log.info("load 1 log cost {} ns", (System.nanoTime() - begin));
        assertEquals(1, entry.getIndex());
    }

    @Test
    public void loadLogsIntoList() {
        testAppend();
        long begin = System.nanoTime();
        List<Log> res = new ArrayList<>();
        syncLogFile.loadLogsIntoList(FileConstant.LOG_FILE_HEADER_SIZE, syncLogFile.size(), res);
        log.info("load {} Logs cost {} ns", res.size(), (System.nanoTime() - begin));
        Assert.assertEquals(100000, res.size());
    }

    @Test
    public void read() {
        testAppend();
        long begin = System.nanoTime();
        ByteBuffer[] read = syncLogFile.read();
        int count = 0;
        for (ByteBuffer byteBuffer : read) {
            count += byteBuffer.limit();
        }
        log.info("load {} bytes cost {} ns", count, (System.nanoTime() - begin));
        Assert.assertEquals(count+FileConstant.LOG_FILE_HEADER_SIZE, syncLogFile.size());
    }

    @Test
    public void transferTo() throws IOException {
        Pool<ByteBuffer> bufferPool = new DirectByteBufferPool(10, 1024 * 1024, 10 * 1024 * 1024);
        File file = File.createTempFile("SyncLogFileDest-", ".txt");
        SyncLogFile dest = new SyncLogFile(file, bufferPool);
        syncLogFile.transferTo(FileConstant.LOG_FILE_HEADER_SIZE, dest);
        Assert.assertEquals(syncLogFile.size(), dest.size());
    }

    @Test
    public void removeAfter() {
        syncLogFile.removeAfter(FileConstant.LOG_FILE_HEADER_SIZE);
        Assert.assertEquals(FileConstant.LOG_FILE_HEADER_SIZE, syncLogFile.size());
    }

}