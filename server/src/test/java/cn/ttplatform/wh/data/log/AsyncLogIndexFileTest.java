package cn.ttplatform.wh.data.log;

import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.data.FileConstant;
import cn.ttplatform.wh.data.support.LogIndexFileMetadataRegion;
import cn.ttplatform.wh.support.DirectByteBufferPool;
import cn.ttplatform.wh.support.Pool;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

@Slf4j
public class AsyncLogIndexFileTest {

    AsyncLogIndexFile asyncLogIndexFile;
    Pool<ByteBuffer> bufferPool;

    @Before
    public void setUp() throws Exception {
        bufferPool = new DirectByteBufferPool(10, 1024 * 1024, 10 * 1024 * 1024);
        File file = File.createTempFile("AsyncLogIndexFileTest-", ".txt");
        File metaFile = File.createTempFile("AsyncLogIndexMetaFileTest-", ".txt");
        LogIndexFileMetadataRegion logIndexFileMetadataRegion = new LogIndexFileMetadataRegion(metaFile);
        asyncLogIndexFile = new AsyncLogIndexFile(file, new ServerProperties(), bufferPool, 0, logIndexFileMetadataRegion);
    }

    @After
    public void tearDown() {
        asyncLogIndexFile.close();
    }

    @Test
    public void getLastLogMetaData() {
        asyncLogIndexFile.append(LogFactory.createEntry(1, 1, 1, new byte[0]), 0);
        LogIndex lastEntryIndex = asyncLogIndexFile.getLastLogMetaData();
        assertEquals(1, asyncLogIndexFile.getMinIndex());
        assertEquals(1, asyncLogIndexFile.getMaxIndex());
        assertEquals(1, lastEntryIndex.getIndex());
        assertEquals(1, lastEntryIndex.getTerm());
        assertEquals(1, lastEntryIndex.getType());
        assertEquals(0, lastEntryIndex.getOffset());
    }

    @Test
    public void getLogMetaData() {
        asyncLogIndexFile.append(LogFactory.createEntry(1, 1, 1, new byte[0]), 0);
        LogIndex entryMetaData = asyncLogIndexFile.getLogMetaData(1);
        assertEquals(1, entryMetaData.getIndex());
        assertEquals(0, entryMetaData.getOffset());
        entryMetaData = asyncLogIndexFile.getLogMetaData(2);
        assertNull(entryMetaData);
    }

    @Test
    public void getEntryOffset() {
        asyncLogIndexFile.append(LogFactory.createEntry(1, 1, 1, new byte[0]),0);
        long entryOffset = asyncLogIndexFile.getLogOffset(1);
        assertEquals(0, entryOffset);
        entryOffset = asyncLogIndexFile.getLogOffset(2);
        assertEquals(-1, entryOffset);
    }

    @Test
    public void testAppend() {
        int capacity = 100000;
        List<Log> logs = new ArrayList<>(capacity);
        long[] offsets = new long[capacity];
        IntStream.range(0, capacity).forEach(index -> {
            logs.add(LogFactory.createEntry(1, 1, index + 1, new byte[0]));
            offsets[index] = index;
        });
        long begin = System.nanoTime();
        asyncLogIndexFile.append(logs, offsets);
        log.info("append {} log indices cost {} ns", capacity, (System.nanoTime() - begin));
        Assert.assertEquals(1, asyncLogIndexFile.getMinIndex());
        Assert.assertEquals(capacity, asyncLogIndexFile.getMaxIndex());
    }

    @Test
    public void removeAfter() {
        testAppend();
        asyncLogIndexFile.removeAfter(1);
        Assert.assertEquals(asyncLogIndexFile.getMinIndex(), asyncLogIndexFile.getMaxIndex());
    }

}