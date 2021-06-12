package cn.ttplatform.wh.data.log;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author Wang Hao
 * @date 2021/6/8 23:48
 */
public interface LogOperation {

    /**
     * index(4 bytes) + term(4 bytes) + type(4 bytes) + commandLength(4 bytes) = 16
     */
    int LOG_ENTRY_HEADER_SIZE = 4 + 4 + 4 + 4;

    long append(Log log);

    long[] append(List<Log> logs);

    void append(ByteBuffer byteBuffer);

    /**
     * read a byte array from file start to end, then transfer to LogEntry
     *
     * @param start start offset
     * @param end   end offset
     * @return an log entry
     */
    Log getLog(long start, long end);

    void loadLogsIntoList(long start, long end, List<Log> res);

    ByteBuffer[] read();

    void transferTo(long offset, LogOperation dst);

    void removeAfter(long offset);

    void delete();

    void close();

    long size();

    boolean isEmpty();
}
