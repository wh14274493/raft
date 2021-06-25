package cn.ttplatform.wh.data.log;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author Wang Hao
 * @date 2021/6/11 22:46
 */
public interface LogIndexOperation {

    void initialize();

    int getMaxIndex();

    int getMinIndex();

    LogIndex getLastLogMetaData();

    LogIndex getLogMetaData(int index);

    long getEntryOffset(int index);

    void append(Log log, long offset);

    void append(List<Log> logs, long[] offsets);

    void append(ByteBuffer byteBuffer);

    void removeAfter(int index);

    boolean isEmpty();

    long size();

    void close();
}
