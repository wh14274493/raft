package cn.ttplatform.wh.core.log;

import static cn.ttplatform.wh.core.support.ByteConvertor.bytesToInt;

import cn.ttplatform.wh.constant.ExceptionMessage;
import cn.ttplatform.wh.constant.FileName;
import cn.ttplatform.wh.core.log.entry.FileLogEntry;
import cn.ttplatform.wh.core.log.entry.FileLogEntryIndex;
import cn.ttplatform.wh.core.log.entry.LogEntry;
import cn.ttplatform.wh.core.log.entry.LogEntryIndex;
import cn.ttplatform.wh.core.log.entry.LogFactory;
import cn.ttplatform.wh.exception.OperateFileException;
import cn.ttplatform.wh.support.BufferPool;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/5 13:58
 */
@Slf4j
@Setter
@Getter
public class YoungGeneration extends AbstractGeneration {

    private String snapshotSource;
    private final LinkedList<LogEntry> pending = new LinkedList<>();

    public YoungGeneration(File parent, BufferPool<ByteBuffer> pool, int lastIncludeIndex) {
        super(new File(parent, FileName.GENERATING_FILE_NAME), pool, false);
        this.fileLogEntry = new FileLogEntry(file, pool);
        this.fileLogEntryIndex = new FileLogEntryIndex(file, pool, lastIncludeIndex);
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
        log.info("prepare to rename oldFileName[{}] to newFileName[{}]", this.file.getPath(), newFile.getPath());
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
        log.info("commit log that index <= {}", commitIndex);
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
        log.debug("sublist from {} to {}", from, to);
        if (isEmpty()) {
            return Collections.emptyList();
        }
        int lastIndex = getLastLogMetaData().getIndex();
        if (from >= to || from > lastIndex) {
            return Collections.emptyList();
        }
        from = Math.max(fileLogEntryIndex.getMinLogIndex(), from);
        to = Math.min(to, lastIndex);
        List<LogEntry> res = new ArrayList<>();
        int maxLogIndex = fileLogEntryIndex.getMaxLogIndex();
        if (from > maxLogIndex) {
            from = from - maxLogIndex - 1;
            to = to - maxLogIndex - 1;
            for (int i = from; i < to; i++) {
                res.add(pending.get(i));
            }
            return res;
        }
        long start = fileLogEntryIndex.getEntryOffset(from);
        long end;
        if (to <= maxLogIndex) {
            end = fileLogEntryIndex.getEntryOffset(to);
        } else {
            end = -1L;
        }
        byte[] entries = fileLogEntry.loadEntriesFromFile(start, end);
        LogFactory logFactory = LogFactory.getInstance();
        int offset = 0;
        while (offset < entries.length) {
            int index = bytesToInt(entries, offset);
            offset += 4;
            int term = bytesToInt(entries, offset);
            offset += 4;
            int type = bytesToInt(entries, offset);
            offset += 4;
            int cmdLength = bytesToInt(entries, offset);
            offset += 4;
            byte[] cmd = Arrays.copyOfRange(entries, offset, offset + cmdLength);
            offset += cmdLength;
            res.add(logFactory.createEntry(type, term, index, cmd));
        }
        offset = to - maxLogIndex - 1;
        for (int i = 0; i < offset; i++) {
            res.add(pending.get(i));
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

    public void clearSnapshot(){
        fileSnapshot.clear();
    }

    public boolean isEmpty() {
        return pending.isEmpty() && (fileLogEntryIndex.isEmpty() || fileLogEntry.isEmpty());
    }

    @Override
    public void close() {
        super.close();
        fileLogEntry.close();
        fileLogEntryIndex.close();
    }
}
