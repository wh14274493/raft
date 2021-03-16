package cn.ttplatform.wh.core.log;

import cn.ttplatform.wh.constant.FileName;
import cn.ttplatform.wh.core.log.snapshot.FileSnapshot;
import cn.ttplatform.wh.core.log.entry.FileLogEntry;
import cn.ttplatform.wh.core.log.entry.FileLogEntryIndex;
import cn.ttplatform.wh.domain.entry.LogEntry;
import java.io.File;

/**
 * @author Wang Hao
 * @date 2021/2/14 16:23
 */
public abstract class AbstractGeneration implements Generation {

    File file;
    FileSnapshot fileSnapshot;
    FileLogEntry fileLogEntry;
    FileLogEntryIndex fileLogEntryIndex;

    AbstractGeneration(File file) {
        this.file = file;
        this.fileSnapshot = new FileSnapshot(new File(file, FileName.SNAPSHOT_FILE_NAME));
        this.fileLogEntry = new FileLogEntry(new File(file, FileName.LOG_ENTRY_FILE_NAME));
        this.fileLogEntryIndex = new FileLogEntryIndex(new File(file, FileName.INDEX_FILE_NAME));
    }

    public int getLastIncludeIndex() {
        return fileSnapshot.getLastIncludeIndex();
    }

    public int getLastIncludeTerm() {
        return fileSnapshot.getLastIncludeTerm();
    }

    public long snapshotSize() {
        return fileSnapshot.getSize();
    }

    public void appendLogEntry(LogEntry logEntry) {
        long offset = fileLogEntry.append(logEntry);
        fileLogEntryIndex.append(logEntry.getIndex(), offset, logEntry.getType(), logEntry.getTerm());
    }

    @Override
    public void close() {
        fileSnapshot.close();
        fileLogEntry.close();
        fileLogEntryIndex.close();
    }
}
