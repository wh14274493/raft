package cn.ttplatform.wh.data.log;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.DSYNC;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import cn.ttplatform.wh.data.tool.Bits;
import cn.ttplatform.wh.exception.IncorrectLogIndexNumberException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

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

    void transferTo(int index, Path dst) throws IOException;

    boolean isEmpty();

    long size();

    void close();
}
