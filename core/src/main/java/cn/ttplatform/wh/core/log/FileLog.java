package cn.ttplatform.wh.core.log;

import cn.ttplatform.wh.constant.FileName;
import cn.ttplatform.wh.core.connector.message.AppendLogEntriesMessage;
import cn.ttplatform.wh.core.connector.message.InstallSnapshotMessage;
import cn.ttplatform.wh.core.connector.message.Message;
import cn.ttplatform.wh.core.log.entry.LogEntry;
import cn.ttplatform.wh.core.support.BufferPool;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/4 16:55
 */
@Slf4j
public class FileLog implements Log {

    private static final Pattern DIR_NAME_PATTERN = Pattern.compile("log-(\\d+)");
    private YoungGeneration youngGeneration;
    private OldGeneration oldGeneration;
    private int commitIndex;
    private int nextIndex;
    private BufferPool<ByteBuffer> pool;

    public FileLog(File parent, BufferPool<ByteBuffer> pool) {
        this.pool = pool;
        oldGeneration = new OldGeneration(getLatestGeneration(parent), pool);
        youngGeneration = new YoungGeneration(parent, pool, oldGeneration.getLastIncludeIndex());
        initialize();
    }

    private void initialize() {
        if (youngGeneration.isEmpty()) {
            commitIndex = oldGeneration.getLastIncludeIndex();
        } else {
            commitIndex = youngGeneration.getMaxLogIndex();
        }
        nextIndex = commitIndex + 1;
    }

    private File getLatestGeneration(File parent) {
        File[] files = parent.listFiles();
        if (files == null || files.length == 0) {
            return new File(parent, FileName.EMPTY_FILE_NAME);
        }
        Optional<File> fileOptional = Arrays.stream(files)
            .filter(file -> DIR_NAME_PATTERN.matcher(file.getName()).matches()).min((o1, o2) -> {
                String o1Name = o1.getName();
                String[] o1Pieces = o1Name.split("-");
                String o2Name = o2.getName();
                String[] o2Pieces = o2Name.split("-");
                return Integer.parseInt(o2Pieces[1]) - Integer.parseInt(o1Pieces[1]);
            });
        return fileOptional.orElse(new File(parent, FileName.EMPTY_FILE_NAME));
    }

    @Override
    public int getLastIncludeIndex() {
        return oldGeneration.getLastIncludeIndex();
    }

    @Override
    public int getLastLogIndex() {
        if (youngGeneration.isEmpty()) {
            return oldGeneration.getLastIncludeIndex();
        }
        return youngGeneration.getLastLogMetaData().getIndex();
    }

    @Override
    public int getLastLogTerm() {
        if (youngGeneration.isEmpty()) {
            return oldGeneration.getLastIncludeTerm();
        }
        return youngGeneration.getLastLogMetaData().getTerm();
    }

    @Override
    public boolean isNewerThan(int logIndex, int term) {
        return term < getLastLogTerm() || (term == getLastLogTerm() && logIndex < getLastLogIndex());
    }

    @Override
    public int getCommitIndex() {
        return commitIndex;
    }

    @Override
    public int getNextIndex() {
        return nextIndex;
    }

    @Override
    public LogEntry getEntry(int index) {
        return youngGeneration.getEntry(index);
    }

    @Override
    public List<LogEntry> subList(int from, int to) {
        return youngGeneration.subList(from, to);
    }

    @Override
    public List<LogEntry> subListWithMaxLength(int from, int maxLength) {
        return subList(from, from + maxLength);
    }

    @Override
    public boolean appendEntries(int index, int term, List<LogEntry> entries) {
        if (checkIndexAndTermIfMatched(index, term)) {
            youngGeneration.removeAfter(index);
            if (entries != null && !entries.isEmpty()) {
                appendEntries(entries);
                log.debug("append {} log entries to pending list", entries.size());
            }
            return true;
        }
        log.debug("append log entries to pending list failed");
        return false;
    }

    private boolean checkIndexAndTermIfMatched(int index, int term) {
        int lastIncludeIndex = oldGeneration.getLastIncludeIndex();
        int lastIncludeTerm = oldGeneration.getLastIncludeTerm();
        if (index < lastIncludeIndex) {
            return false;
        }
        if (index == lastIncludeIndex) {
            return term == lastIncludeTerm;
        }
        LogEntry logEntry = getEntry(index);
        if (logEntry == null) {
            return false;
        }
        return term == logEntry.getTerm();
    }

    private void appendEntries(List<LogEntry> entries) {
        entries.forEach(entry -> youngGeneration.pendingLogEntry(entry));
        updateNextIndex();
    }

    @Override
    public void appendEntry(LogEntry entry) {
        youngGeneration.pendingLogEntry(entry);
        updateNextIndex();
    }

    private void updateNextIndex() {
        if (youngGeneration.isEmpty()) {
            nextIndex = oldGeneration.getLastIncludeIndex() + 1;
        } else {
            nextIndex = youngGeneration.getLastLogMetaData().getIndex() + 1;
        }
        log.debug("update nextIndex[{}]", nextIndex);
    }

    @Override
    public Message createAppendLogEntriesMessage(String leaderId, int term, int nextIndex, int size) {
        int lastIncludeIndex = oldGeneration.getLastIncludeIndex();
        int lastIncludeTerm = oldGeneration.getLastIncludeTerm();
        if (nextIndex <= lastIncludeIndex) {
            return null;
        }
        AppendLogEntriesMessage message = AppendLogEntriesMessage.builder()
            .leaderCommitIndex(commitIndex)
            .term(term)
            .leaderId(leaderId)
            .logEntries(subListWithMaxLength(nextIndex, size))
            .build();
        int preIndex = lastIncludeIndex;
        int preTerm = lastIncludeTerm;
        if (nextIndex - 1 > lastIncludeIndex) {
            LogEntry logEntry = getEntry(nextIndex - 1);
            preIndex = logEntry.getIndex();
            preTerm = logEntry.getTerm();
        }
        message.setPreLogIndex(preIndex);
        message.setPreLogTerm(preTerm);
        return message;
    }

    @Override
    public Message createInstallSnapshotMessage(int term, long offset, int size) {
        return InstallSnapshotMessage.builder().term(term)
            .lastIncludeIndex(oldGeneration.getLastIncludeIndex())
            .lastIncludeTerm(oldGeneration.getLastIncludeTerm())
            .chunk(oldGeneration.readSnapshot(offset, size))
            .done(offset + size >= oldGeneration.getSnapshotSize())
            .build();
    }

    @Override
    public List<LogEntry> advanceCommitIndex(int newCommitIndex, int term) {
        if (newCommitIndex <= commitIndex) {
            return Collections.emptyList();
        }
        LogEntry entry = getEntry(newCommitIndex);
        if (entry == null || entry.getTerm() < term) {
            return Collections.emptyList();
        }
        commitIndex = newCommitIndex;
        return youngGeneration.commit(commitIndex);
    }

    @Override
    public boolean shouldGenerateSnapshot(int snapshotGenerateThreshold) {
        return youngGeneration.getLogEntryFileSize() >= snapshotGenerateThreshold;
    }

    @Override
    public boolean installSnapshot(InstallSnapshotMessage message) {
        int lastIncludeIndex = message.getLastIncludeIndex();
        int lastIncludeTerm = message.getLastIncludeTerm();
        long offset = message.getOffset();
        if (lastIncludeIndex != youngGeneration.getLastIncludeIndex()
            || lastIncludeTerm != youngGeneration.getLastIncludeTerm()
            || offset != youngGeneration.getSnapshotSize()) {
            return false;
        }
        youngGeneration.writeSnapshot(message.getChunk());
        if (message.isDone()) {
            replace(lastIncludeIndex, lastIncludeTerm);
        }
        return true;
    }

    @Override
    public void generateSnapshot(int lastIncludeIndex, int lastIncludeTerm, byte[] content) {
        youngGeneration.generateSnapshot(lastIncludeIndex, lastIncludeTerm, content);
        replace(lastIncludeIndex, lastIncludeTerm);
    }

    private void replace(int lastIncludeIndex, int lastIncludeTerm) {
        List<LogEntry> logEntries = subList(lastIncludeIndex + 1, nextIndex);
        oldGeneration.close();
        youngGeneration.close();
        File file = youngGeneration.rename(lastIncludeIndex, lastIncludeTerm);
        oldGeneration = new OldGeneration(file, pool);
        youngGeneration = new YoungGeneration(file.getParentFile(), pool,oldGeneration.getLastIncludeIndex());
        appendEntries(logEntries);
    }

    @Override
    public byte[] getSnapshotData() {
        return oldGeneration.readSnapshot();
    }
}
