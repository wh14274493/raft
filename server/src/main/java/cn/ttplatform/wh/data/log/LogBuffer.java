package cn.ttplatform.wh.data.log;

import cn.ttplatform.wh.GlobalContext;
import cn.ttplatform.wh.data.FileConstant;
import cn.ttplatform.wh.data.pool.BlockCache;
import cn.ttplatform.wh.support.Pool;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/6/8 23:50
 */
@Slf4j
public class LogBuffer implements LogOperation {

    private final BlockCache blockCache;

    public LogBuffer(File file, GlobalContext context) {
        this.blockCache = new BlockCache(context, file, FileConstant.LOG_FILE_HEADER_SIZE);
    }

    @Override
    public long append(Log log) {
        long offset = blockCache.getSize();
        blockCache.appendInt(log.getIndex());
        blockCache.appendInt(log.getTerm());
        blockCache.appendInt(log.getType());
        byte[] command = log.getCommand();
        blockCache.appendInt(command.length);
        blockCache.appendBytes(command);
        return offset;
    }

    @Override
    public long[] append(List<Log> logs) {
        long[] offsets = new long[logs.size()];
        for (int i = 0; i < logs.size(); i++) {
            offsets[i] = blockCache.getSize();
            append(logs.get(i));
        }
        return offsets;
    }

    @Override
    public void append(ByteBuffer byteBuffer) {
        blockCache.appendBlock(byteBuffer);
    }

    @Override
    public Log getLog(long start, long end) {
        int index = blockCache.getInt(start);
        start += 4;
        int term = blockCache.getInt(start);
        start += 4;
        int type = blockCache.getInt(start);
        start += 4;
        int cmdLength = blockCache.getInt(start);
        start += 4;
        byte[] cmd = null;
        if (cmdLength > 0) {
            cmd = new byte[cmdLength];
            blockCache.get(start, cmd);
        }
        return LogFactory.createEntry(type, term, index, cmd, cmdLength);
    }

    @Override
    public void loadLogsIntoList(long start, long end, List<Log> res) {
        res.add(getLog(start, end));
    }

    @Override
    public ByteBuffer[] read() {
        return blockCache.getBlocks(0L);
    }

    @Override
    public void transferTo(long offset, LogOperation logOperation) {
        ByteBuffer[] buffers = blockCache.getBlocks(offset);
        Arrays.stream(buffers).forEach(logOperation::append);
    }

    @Override
    public void removeAfter(long offset) {
        blockCache.removeAfter(offset);
    }

    @Override
    public void close() {
        blockCache.close();
    }

    @Override
    public long size() {
        return blockCache.getSize();
    }

    @Override
    public boolean isEmpty() {
        return blockCache.getSize() <= FileConstant.LOG_FILE_HEADER_SIZE;
    }


}
