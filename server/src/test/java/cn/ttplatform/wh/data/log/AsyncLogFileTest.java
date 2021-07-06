package cn.ttplatform.wh.data.log;

import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.data.FileConstant;
import cn.ttplatform.wh.data.index.LogIndex;
import cn.ttplatform.wh.support.DirectByteBufferPool;
import cn.ttplatform.wh.support.HeapByteBufferPool;
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

@Slf4j
public class AsyncLogFileTest {

    AsyncLogFile asyncLogFile;
    Pool<ByteBuffer> bufferPool;
    LogFileMetadataRegion logFileMetadataRegion;
    LogFileMetadataRegion generatingLogFileMetadataRegion;

    @Before
    public void setUp() throws Exception {
        bufferPool = new HeapByteBufferPool(10, 1024 * 1024, 10 * 1024 * 1024);
        File file = File.createTempFile("AsyncLogFile-", ".txt");
        File metaFile = File.createTempFile("AsyncLogMetaFile-", ".txt");
        this.logFileMetadataRegion = FileConstant.getLogFileMetadataRegion(metaFile);
        this.generatingLogFileMetadataRegion = FileConstant.getGeneratingLogFileMetadataRegion(metaFile);
        this.asyncLogFile = new AsyncLogFile(file, new ServerProperties(), bufferPool, logFileMetadataRegion);
    }

    @After
    public void tearDown() {
        asyncLogFile.close();
    }

    @Test
    public void append() {
        byte[] content = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        long begin = System.nanoTime();
        asyncLogFile.append(LogFactory.createEntry(1, 1, 1, content));
        log.info("append 1 log cost {} ns", System.nanoTime() - begin);
        Assert.assertEquals(Log.HEADER_BYTES + content.length, asyncLogFile.size());
    }

    @Test
    public void testAppend() {
        byte[] content = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        int capacity = 100000;
        List<Log> logEntries = new ArrayList<>(capacity);
        IntStream.range(0, capacity).forEach(index -> logEntries.add(LogFactory.createEntry(1, 1, index + 1, content)));
        long begin = System.nanoTime();
        long[] append = asyncLogFile.append(logEntries);
        log.info("append {} logs cost {} ns", capacity, System.nanoTime() - begin);
        assertEquals((long) capacity * (Log.HEADER_BYTES + content.length), asyncLogFile.size());
    }

    @Test
    public void getLog() {
        byte[] content = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        asyncLogFile.append(LogFactory.createEntry(1, 1, 1, content));
        long begin = System.nanoTime();
        Log log = asyncLogFile.getLog(0, asyncLogFile.size());
        LogIndex logIndex = log.getMetadata();
        AsyncLogFileTest.log.info("load 1 log cost {} ns", (System.nanoTime() - begin));
        assertEquals(1, logIndex.getIndex());
        assertEquals(1, logIndex.getTerm());
        assertEquals(1, logIndex.getType());
        assertEquals(content.length, log.getCommand().length);
    }

    @Test
    public void loadLogsIntoList() {
        testAppend();
        long begin = System.nanoTime();
        List<Log> res = new ArrayList<>(100000);
        asyncLogFile.loadLogsIntoList(0, asyncLogFile.size(), res);
        log.info("load {} logs cost {} ns", res.size(), (System.nanoTime() - begin));
        Assert.assertEquals(100000, res.size());
    }

    @Test
    public void read() {
        testAppend();
        long begin = System.nanoTime();
        ByteBuffer[] read = asyncLogFile.read();
        int count = 0;
        for (ByteBuffer byteBuffer : read) {
            count += byteBuffer.limit();
        }
        log.info("load {} bytes cost {} ns", count, (System.nanoTime() - begin));
        Assert.assertEquals(count, asyncLogFile.size());
    }

    @Test
    public void transferTo() throws IOException {
        testAppend();
        File file = File.createTempFile("AsyncLogFileDest-", ".txt");
        AsyncLogFile dest = new AsyncLogFile(file, new ServerProperties(), bufferPool, generatingLogFileMetadataRegion);
        long begin = System.nanoTime();
        asyncLogFile.transferTo(0, dest);
        log.info("transferTo {} bytes cost {} ns", asyncLogFile.size(), (System.nanoTime() - begin));
        Assert.assertEquals(asyncLogFile.size(), dest.size());
    }

    @Test
    public void removeAfter() {
        testAppend();
        long size = asyncLogFile.size();
        long begin = System.nanoTime();
        asyncLogFile.removeAfter(0);
        log.info("remove {} bytes cost cost {} ns", size, (System.nanoTime() - begin));
        Assert.assertEquals(0, asyncLogFile.size());
    }
}