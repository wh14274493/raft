package cn.ttplatform.wh.core.log;

import cn.ttplatform.wh.domain.entry.LogEntry;
import cn.ttplatform.wh.domain.entry.LogEntryIndex;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/5 13:57
 */
@Slf4j
public class OldGeneration extends AbstractGeneration {

    private final List<LogEntry> pending = new LinkedList<>();

    public OldGeneration(File file) {
        super(file);
    }

    public int getMaxLogIndex() {
        return fileLogEntryIndex.getMaxLogIndex();
    }

    public int getLastLogIndex() {
        return getLastEntryIndex().getIndex();
    }

    public int getLastLogTerm() {
        return getLastEntryIndex().getTerm();
    }

    public LogEntryIndex getLastEntryIndex() {
        if (pending.isEmpty()) {
            return fileLogEntryIndex.getLastEntryIndex();
        }
        LogEntry logEntry = Objects.requireNonNull(getLastElement());
        return LogEntryIndex.builder().index(logEntry.getIndex()).term(logEntry.getTerm()).type(logEntry.getType())
            .build();
    }


    private LogEntry getLastElement() {
        return pending.isEmpty() ? null : pending.get(pending.size() - 1);
    }

    public void pendingLogEntry(LogEntry logEntry) {
        pending.add(logEntry);
    }

    public List<LogEntry> commit(int commitIndex) {
        int maxLogIndex = fileLogEntryIndex.getMaxLogIndex();
        int size = commitIndex - maxLogIndex;
        List<LogEntry> committedEntries = new ArrayList<>(size);
        Iterator<LogEntry> iterator = pending.iterator();
        while (iterator.hasNext()) {
            LogEntry logEntry = iterator.next();
            if (logEntry.getIndex() <= commitIndex) {
                appendLogEntry(logEntry);
                committedEntries.add(logEntry);
                iterator.remove();
            }
        }
        return committedEntries;
    }


    public LogEntry getEntry(int index) {
        if (index < fileLogEntryIndex.getMinLogIndex()) {
            log.debug("index[{}] < minEntryIndex[{}]", index, fileLogEntryIndex.getMinLogIndex());
            return null;
        }
        if (index > fileLogEntryIndex.getMaxLogIndex()) {
            log.debug("index[{}] > maxEntryIndex[{}]", index, fileLogEntryIndex.getMaxLogIndex());
            return pending.get(index - fileLogEntryIndex.getMaxLogIndex() - 1);
        }
        long entryOffset = fileLogEntryIndex.getEntryOffset(index);
        return fileLogEntry.load(entryOffset);
    }

    public List<LogEntry> subList(int from, int to) {
        int minEntryIndex = fileLogEntryIndex.getMinLogIndex();
        int maxEntryIndex = fileLogEntryIndex.getMaxLogIndex();
        LogEntry lastElement = getLastElement();
        int lastEntryIndex = lastElement == null ? maxEntryIndex : lastElement.getIndex();
        if (from > to || from > lastEntryIndex) {
            return Collections.emptyList();
        }
        List<LogEntry> res = new ArrayList<>();
        from = Math.max(from, minEntryIndex);
        if (to <= maxEntryIndex + 1) {
            for (int index = from; index < to; index++) {
                res.add(getEntry(index));
            }
            return res;
        }
        if (from > maxEntryIndex) {
            to = Math.min(to, lastEntryIndex);
            for (int index = from - maxEntryIndex - 1; index < to; index++) {
                res.add(pending.get(index));
            }
            return res;
        }
        for (int index = from; index < maxEntryIndex + 1; index++) {
            res.add(getEntry(index));
        }
        to = Math.min(to, lastEntryIndex);
        for (int index = from - maxEntryIndex - 1; index < to; index++) {
            res.add(pending.get(index));
        }
        return res;
    }

    public byte[] readSnapshot(long start, int size) {
        return fileSnapshot.read(start, size);
    }

    public long getEntryFileSize() {
        return fileLogEntry.size();
    }

}
