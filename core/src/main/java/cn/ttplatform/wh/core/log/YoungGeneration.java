package cn.ttplatform.wh.core.log;

import cn.ttplatform.wh.constant.ExceptionMessage;
import cn.ttplatform.wh.constant.FileName;
import cn.ttplatform.wh.core.support.DirectByteBufferPool;
import cn.ttplatform.wh.core.log.entry.FileLogEntry;
import cn.ttplatform.wh.core.log.entry.FileLogEntryIndex;
import cn.ttplatform.wh.core.log.entry.LogEntry;
import cn.ttplatform.wh.core.log.entry.LogEntryIndex;
import cn.ttplatform.wh.exception.OperateFileException;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/5 13:58
 */
@Slf4j
public class YoungGeneration extends AbstractGeneration {

    private final LinkedList<LogEntry> pending = new LinkedList<>();

    public YoungGeneration(File parent) {
        super(new File(parent, FileName.GENERATING_FILE_NAME));
        this.fileLogEntry = new FileLogEntry(file);
        this.fileLogEntryIndex = new FileLogEntryIndex(file);
    }

    public YoungGeneration(File parent, DirectByteBufferPool pool) {
        super(new File(parent, FileName.GENERATING_FILE_NAME), pool);
        this.fileLogEntry = new FileLogEntry(file, pool);
        this.fileLogEntryIndex = new FileLogEntryIndex(file, pool);
    }

    /**
     * @param lastIncludeIndex the index of last entry included in snapshot file
     * @param lastIncludeTerm  the term of last entry included in snapshot file
     * @param content          snapshot data
     */
    public void generateSnapshot(int lastIncludeIndex, int lastIncludeTerm, byte[] content) {
        fileSnapshot.write(lastIncludeIndex, lastIncludeTerm, content);
    }

    /**
     * After the log snapshot is generated, the young generation needs to be promoted to the old generation. We use the
     * method of changing the file name to avoid copying the file.
     *
     * @param lastIncludeIndex the index of last entry included in snapshot file
     * @param lastIncludeTerm  the term of last entry included in snapshot file
     * @return Renamed file
     */
    public File rename(int lastIncludeIndex, int lastIncludeTerm) {
        String newFileName = FileName.GENERATION_FILE_NAME_PREFIX + lastIncludeTerm + '-' + lastIncludeIndex;
        File newFile = new File(this.file.getParent(), newFileName);
        log.debug("prepare to rename oldFileName[{}] to newFileName[{}]", this.file.getPath(), newFile.getPath());
        if (!this.file.renameTo(newFile)) {
            throw new OperateFileException(ExceptionMessage.RENAME_FILE_ERROR);
        }
        return newFile;
    }

    public LogEntryIndex getLastLogMetaData() {
        if (!pending.isEmpty()) {
            LogEntry last = pending.getLast();
            return LogEntryIndex.builder().index(last.getIndex()).term(last.getTerm()).offset(-1L).type(last.getType())
                .build();
        }
        if (!fileLogEntryIndex.isEmpty()) {
            return fileLogEntryIndex.getLastEntryIndex();
        }
        return null;
    }

    public int getMaxLogIndex() {
        return fileLogEntryIndex.getMaxLogIndex();
    }

    /**
     * Save log in memory.
     *
     * @param logEntry Logs to be submitted
     */
    public void pendingLogEntry(LogEntry logEntry) {
        pending.add(logEntry);
    }

    /**
     * Save the log to a file and update the index file at the same time.
     *
     * @param logEntry Log to be submitted
     */
    public void appendLogEntry(LogEntry logEntry) {
        long offset = fileLogEntry.append(logEntry);
        fileLogEntryIndex.append(logEntry, offset);
    }

    /**
     * Save the logs to a file and update the index file at the same time.
     *
     * @param logEntries Logs to be submitted
     */
    public void appendLogEntries(List<LogEntry> logEntries) {
        List<Long> offsetList = fileLogEntry.append(logEntries);
        fileLogEntryIndex.append(logEntries, offsetList);
    }

    /**
     * According to the {@param commitIndex} position, save the logs with index numbers less than {@param commitIndex}
     * in the memory to a file, and return a list containing these logs.
     *
     * @param commitIndex commitIndex
     * @return a list containing committed logs
     */
    public List<LogEntry> commit(int commitIndex) {
        int maxLogIndex = fileLogEntryIndex.getMaxLogIndex();
        int size = commitIndex - maxLogIndex;
        List<LogEntry> committedEntries = new ArrayList<>(size);
        while (!pending.isEmpty() && pending.peekFirst().getIndex() <= commitIndex) {
            committedEntries.add(pending.pollFirst());
        }
        appendLogEntries(committedEntries);
        return committedEntries;
    }

    /**
     * Search memory or log in file according to index, and return, if not found, return null
     *
     * @param index index
     * @return Log found
     */
    public LogEntry getEntry(int index) {
        if (isEmpty()) {
            log.debug("young generation is empty.");
            return null;
        }
        if (index < fileLogEntryIndex.getMinLogIndex()) {
            log.debug("index[{}] < minEntryIndex[{}]", index, fileLogEntryIndex.getMinLogIndex());
            return null;
        }
        if (index > getLastLogMetaData().getIndex()) {
            log.debug("index[{}] > lastLogIndex", index);
            return null;
        }
        if (index > fileLogEntryIndex.getMaxLogIndex()) {
            log.debug("index[{}] > maxEntryIndex[{}]", index, fileLogEntryIndex.getMaxLogIndex());
            return pending.get(index - fileLogEntryIndex.getMaxLogIndex() - 1);
        }
        long startOffset = fileLogEntryIndex.getEntryOffset(index);
        long endOffset = fileLogEntryIndex.getEntryOffset(index + 1);
        return fileLogEntry.getEntry(startOffset, endOffset);
    }

    /**
     * Find the log list from the {@param from} position to the {@param to} position, but this list does not contain the
     * {@param to} position
     *
     * @param from start index
     * @param to   end index
     * @return result list
     */
    public List<LogEntry> subList(int from, int to) {
        if (isEmpty()) {
            return Collections.emptyList();
        }
        int lastEntryIndex = getLastLogMetaData().getIndex();
        if (from >= to || from > lastEntryIndex) {
            return Collections.emptyList();
        }
        from = Math.max(from, fileLogEntryIndex.getMinLogIndex());
        to = Math.min(to, lastEntryIndex + 1);
        List<LogEntry> res = new ArrayList<>(to - from);
        while (from < to) {
            res.add(getEntry(from++));
        }
        return res;
    }

    /**
     * Remove log entries after {@param index}
     *
     * @param index start index
     */
    public void removeAfter(int index) {
        if (isEmpty() || index >= getLastLogMetaData().getIndex()) {
            return;
        }
        int maxLogIndex = fileLogEntryIndex.getMaxLogIndex();
        if (index == maxLogIndex) {
            pending.clear();
        } else if (index < maxLogIndex) {
            long offset = fileLogEntryIndex.getEntryOffset(index + 1);
            fileLogEntryIndex.removeAfter(index);
            fileLogEntry.removeAfter(offset);
        } else {
            pending.removeIf(logEntry -> logEntry.getIndex() > index);
        }
    }

    public void writeSnapshot(byte[] chunk) {
        fileSnapshot.append(chunk);
    }

    public boolean isEmpty() {
        return pending.isEmpty() && (fileLogEntryIndex.isEmpty() || fileLogEntry.isEmpty());
    }
}
