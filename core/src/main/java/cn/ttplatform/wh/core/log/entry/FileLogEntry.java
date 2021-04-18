package cn.ttplatform.wh.core.log.entry;

import cn.ttplatform.wh.constant.ExceptionMessage;
import cn.ttplatform.wh.constant.FileName;
import cn.ttplatform.wh.core.log.EntryFactory;
import cn.ttplatform.wh.domain.entry.LogEntry;
import cn.ttplatform.wh.exception.OperateFileException;
import cn.ttplatform.wh.core.common.RandomAccessFileWrapper;
import java.io.File;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午10:12
 */
public class FileLogEntry {

    private static final EntryFactory ENTRYFACTORY = EntryFactory.INSTANCE;

    private final RandomAccessFileWrapper file;

    public FileLogEntry(File parent) {
        this.file = new RandomAccessFileWrapper(new File(parent, FileName.LOG_ENTRY_FILE_NAME));
    }

    public long append(LogEntry logEntry) {
        long offset = file.size();
        file.seek(offset);
        file.writeInt(logEntry.getType());
        file.writeInt(logEntry.getTerm());
        file.writeInt(logEntry.getIndex());
        file.writeInt(logEntry.getCommand().length);
        file.writeBytes(logEntry.getCommand());
        return offset;
    }

    public LogEntry getEntry(long offset) {
        if (offset < 0 || offset > file.size()) {
            return null;
        }
        try {
            file.seek(offset);
            int type = file.readInt();
            int term = file.readInt();
            int index = file.readInt();
            int length = file.readInt();
            byte[] content = file.readBytes(length);
            return ENTRYFACTORY.createEntry(type, term, index, content);
        } catch (Exception e) {
            throw new OperateFileException(ExceptionMessage.LOAD_ENTRY_ERROR, e.getCause());
        }
    }

    public void removeAfter(long offset) {
        file.truncate(offset < 0 ? 0L : offset);
    }

    public void close() {
        file.close();
    }

    public long size() {
        return file.size();
    }

    public boolean isEmpty() {
        return file.size() == 0L;
    }
}
