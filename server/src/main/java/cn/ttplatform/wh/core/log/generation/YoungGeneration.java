package cn.ttplatform.wh.core.log.generation;

import static cn.ttplatform.wh.core.log.tool.ByteConvertor.bytesToInt;

import cn.ttplatform.wh.constant.ErrorMessage;
import cn.ttplatform.wh.core.log.entry.FileLogEntry;
import cn.ttplatform.wh.core.log.entry.FileLogEntryIndex;
import cn.ttplatform.wh.core.log.entry.LogEntry;
import cn.ttplatform.wh.core.log.entry.LogEntryFactory;
import cn.ttplatform.wh.core.log.entry.LogEntryIndex;
import cn.ttplatform.wh.exception.OperateFileException;
import cn.ttplatform.wh.support.Pool;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;
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

    public YoungGeneration(File parent, Pool<ByteBuffer> byteBufferPool, Pool<byte[]> byteArrayPool, int lastIncludeIndex) {
        super(new File(parent, FileName.GENERATING_FILE_NAME), byteBufferPool, byteArrayPool, false);
        this.fileLogEntry = new FileLogEntry(file, byteBufferPool, byteArrayPool);
        this.fileLogEntryIndex = new FileLogEntryIndex(file, byteBufferPool, byteArrayPool, lastIncludeIndex);
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
     * After the log snapshot is generated, the young generation needs to be promoted to the old generation. We use the method of
     * changing the file name to avoid copying the file.
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
            throw new OperateFileException(ErrorMessage.RENAME_FILE_ERROR);
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
     * @param logEntry Log to be submitted
     */
    public void pendingEntry(LogEntry logEntry) {
        if (!pending.isEmpty() && logEntry.getIndex() != pending.peekLast().getIndex() + 1) {
            throw new IllegalStateException("The index number of the log is incorrect ");
        }
        pending.add(logEntry);
    }

    /**
     * Save logs in memory.
     *
     * @param logEntries Logs to be submitted
     */
    public void pendingEntries(List<LogEntry> logEntries) {
        if (!pending.isEmpty() && logEntries.get(0).getIndex() != pending.peekLast().getIndex() + 1) {
            throw new IllegalStateException("The index number of the log is incorrect ");
        }
        pending.addAll(logEntries);
        log.info("write {} logs into memory.", logEntries.size());
    }

    /**
     * Save the log to a file and update the index file at the same time.
     *
     * @param logEntry Log to be submitted
     */
    public void appendLogEntry(LogEntry logEntry) {
        long offsets = fileLogEntry.append(logEntry);
        fileLogEntryIndex.append(logEntry, offsets);
    }

    /**
     * Save the logs to a file and update the index file at the same time.
     *
     * @param logEntries Logs to be submitted
     */
    public void appendLogEntries(List<LogEntry> logEntries) {
        if (logEntries == null || logEntries.isEmpty()) {
            log.info("logEntries size is 0, skip to write operation.");
            return;
        }
        log.debug("prepare to write {} logs into file.", logEntries.size());
        if (logEntries.size() == 1) {
            appendLogEntry(logEntries.get(0));
        } else {
            long[] offsets = fileLogEntry.append(logEntries);
            fileLogEntryIndex.append(logEntries, offsets);
        }
    }

    /**
     * According to the {@param commitIndex} position, save the logs with index numbers less than {@param commitIndex} in the
     * memory to a file, and return a list containing these logs.
     *
     * @param commitIndex commitIndex
     */
    public void commit(int commitIndex) {
        int maxLogIndex = fileLogEntryIndex.getMaxLogIndex();
        int size = commitIndex - maxLogIndex;
        List<LogEntry> committedEntries = new ArrayList<>(size);
        while (!pending.isEmpty() && pending.peekFirst().getIndex() <= commitIndex) {
            committedEntries.add(pending.pollFirst());
        }
        appendLogEntries(committedEntries);
        log.debug("append {} logs", committedEntries.size());
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
        int lastLogIndex = getLastLogMetaData().getIndex();
        if (index > lastLogIndex) {
            log.debug("index[{}] > lastLogIndex[{}]", index, lastLogIndex);
            return null;
        }
        if (index > fileLogEntryIndex.getMaxLogIndex()) {
            log.debug("index[{}] > maxEntryIndex[{}], find log from pending.", index, fileLogEntryIndex.getMaxLogIndex());
            return pending.get(index - fileLogEntryIndex.getMaxLogIndex() - 1);
        }
        long startOffset = fileLogEntryIndex.getEntryOffset(index);
        long endOffset = fileLogEntryIndex.getEntryOffset(index + 1);
        return fileLogEntry.getEntry(startOffset, endOffset);
    }

    public LogEntryIndex getEntryMetaData(int index) {
        return fileLogEntryIndex.getEntryMetaData(index);
    }

    /**
     * Find the log list from the {@param from} position to the {@param to} position, but this list does not contain the {@param
     * to} position
     *
     * @param from start index
     * @param to   end index
     * @return result list
     */
    public List<LogEntry> subList(int from, int to) {
        if (isEmpty()) {
            return Collections.emptyList();
        }
        int lastIndex = getLastLogMetaData().getIndex();
        if (from >= to || from > lastIndex) {
            return Collections.emptyList();
        }
        from = Math.max(fileLogEntryIndex.getMinLogIndex(), from);
        to = Math.min(to, lastIndex + 1);
        log.debug("sublist from {} to {}", from, to);
        List<LogEntry> res = new ArrayList<>();
        int maxLogIndex = fileLogEntryIndex.getMaxLogIndex();
        if (from > maxLogIndex) {
            from = from - maxLogIndex - 1;
            to = to - maxLogIndex - 1;
            IntStream.range(from, to).forEach(index -> res.add(pending.get(index)));
            return res;
        }
        long start = fileLogEntryIndex.getEntryOffset(from);
        long end;
        if (to <= maxLogIndex) {
            end = fileLogEntryIndex.getEntryOffset(to);
        } else {
            end = fileLogEntry.size();
        }
        byte[] entries = fileLogEntry.loadEntriesFromFile(start, end);
        int size = (int) (end - start);
        int offset = 0;
        while (offset < size) {
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
            res.add(LogEntryFactory.createEntry(type, term, index, cmd));
        }
        to = to - maxLogIndex - 1;
        IntStream.range(0, to).forEach(index -> res.add(pending.get(index)));
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
        if (chunk.length == 0) {
            log.debug("chunk size is 0.");
            return;
        }
        fileSnapshot.append(chunk);
    }

    public void clearSnapshot() {
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
