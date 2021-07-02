package cn.ttplatform.wh.data.log;

import cn.ttplatform.wh.data.log.Log;
import cn.ttplatform.wh.data.log.LogFactory;
import cn.ttplatform.wh.data.log.LogIndex;
import cn.ttplatform.wh.data.log.SyncLogIndexFile;
import cn.ttplatform.wh.data.support.LogIndexFileMetadataRegion;
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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

/**
 * @author Wang Hao
 * @date 2021/4/29 11:56
 */
@Slf4j
public class SyncLogIndexFileTest {

    SyncLogIndexFile syncLogIndexFile;

    @Before
    public void setUp() throws IOException {
        Pool<ByteBuffer> bufferPool = new DirectByteBufferPool(10, 1024 * 1024, 10 * 1024 * 1024);
        File file = File.createTempFile("SyncLogIndexFile-", ".txt");
        File metaFile = File.createTempFile("SyncLogIndexMetaFile-", ".txt");
        LogIndexFileMetadataRegion logIndexFileMetadataRegion = new LogIndexFileMetadataRegion(metaFile);
        syncLogIndexFile = new SyncLogIndexFile(file, logIndexFileMetadataRegion, bufferPool, 0);
    }

    @After
    public void tearDown() {
        syncLogIndexFile.close();
    }

    @Test
    public void getLastLogMetaData() {
        syncLogIndexFile.append(LogFactory.createEntry(1, 1, 1, new byte[0]), 0);
        LogIndex lastEntryIndex = syncLogIndexFile.getLastLogMetaData();
        assertNotNull(lastEntryIndex);
    }

    @Test
    public void getLogOffset() {
        syncLogIndexFile.append(LogFactory.createEntry(1, 1, 1, new byte[0]), 0);
        long entryOffset = syncLogIndexFile.getLogOffset(1);
        assertEquals(0, entryOffset);
        entryOffset = syncLogIndexFile.getLogOffset(2);
        assertEquals(-1, entryOffset);
    }

    @Test
    public void getEntryMetaData() {
        syncLogIndexFile.append(LogFactory.createEntry(1, 1, 1, new byte[0]), 0);
        LogIndex entryMetaData = syncLogIndexFile.getLogMetaData(1);
        assertEquals(0, entryMetaData.getOffset());
        entryMetaData = syncLogIndexFile.getLogMetaData(2);
        assertNull(entryMetaData);
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
        syncLogIndexFile.append(logs, offsets);
        log.info("append {} log indices cost {} ns", capacity, (System.nanoTime() - begin));
        Assert.assertEquals(capacity, syncLogIndexFile.getMaxIndex());
    }

    @Test
    public void removeAfter() {
        testAppend();
        syncLogIndexFile.removeAfter(1);
        Assert.assertEquals(syncLogIndexFile.getMinIndex(), syncLogIndexFile.getMaxIndex());
    }

}