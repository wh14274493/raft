package cn.ttplatform.wh.data.log;

import cn.ttplatform.wh.GlobalContext;
import cn.ttplatform.wh.data.FileConstant;
import cn.ttplatform.wh.data.pool.BlockCache;
import cn.ttplatform.wh.support.Pool;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;

/**
 * @author Wang Hao
 * @date 2021/6/11 22:45
 */
public class LogIndexBuffer implements LogIndexOperation {

    private int minIndex;
    private int maxIndex;
    BlockCache blockCache;
    Pool<ByteBuffer> byteBufferPool;

    public LogIndexBuffer(File file, GlobalContext context) {
        this.blockCache = new BlockCache(context, file, FileConstant.LOG_INDEX_FILE_HEADER_SIZE);
    }

    @Override
    public void initialize() {

    }

    @Override
    public int getMaxIndex() {
        return maxIndex;
    }

    @Override
    public int getMinIndex() {
        return minIndex;
    }

    @Override
    public LogIndex getLastLogMetaData() {
        return null;
    }

    @Override
    public LogIndex getLogMetaData(int index) {
        return null;
    }

    @Override
    public long getEntryOffset(int index) {
        return 0;
    }

    @Override
    public void append(Log log, long offset) {

    }

    @Override
    public void append(List<Log> logs, long[] offsets) {

    }

    @Override
    public void append(ByteBuffer byteBuffer) {

    }

    @Override
    public void removeAfter(int index) {

    }

    @Override
    public void transferTo(int index, Path dst) throws IOException {

    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public void close() {

    }
}
