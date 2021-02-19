package cn.ttplatform.lc.core.store.log;

import cn.ttplatform.lc.core.rpc.message.domain.AppendLogEntriesMessage;
import cn.ttplatform.lc.core.rpc.message.domain.InstallSnapshotMessage;
import cn.ttplatform.lc.core.rpc.message.domain.Message;
import cn.ttplatform.lc.core.store.log.entry.LogEntry;
import java.io.File;
import java.util.List;

/**
 * @author Wang Hao
 * @date 2021/2/4 16:55
 */
public class FileLog implements Log {

    private YoungGeneration youngGeneration;
    private OldGeneration oldGeneration;
    private int commitIndex;
    private int nextIndex;

    @Override
    public int getLastLogIndex() {
        return oldGeneration.getLastLogIndex();
    }

    @Override
    public int getLastLogTerm() {
        return oldGeneration.getLastLogTerm();
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
        return oldGeneration.getEntry(index);
    }

    @Override
    public List<LogEntry> subList(int from, int to) {
        return oldGeneration.subList(from, to);
    }

    @Override
    public List<LogEntry> subListWithMaxLength(int from, int maxLength) {
        return subList(from, from + maxLength);
    }

    @Override
    public boolean appendEntries(int index, int term, List<LogEntry> entries) {
        LogEntry logEntry = oldGeneration.getEntry(index);
        if (term != logEntry.getTerm()) {
            return false;
        }
        if (entries.isEmpty()) {
            return true;
        }
        LogEntry first = entries.get(0);
        if (index != first.getIndex() || term != first.getTerm()) {
            return false;
        }
        entries.forEach(entry -> oldGeneration.pendingLogEntry(entry));
        return true;
    }

    @Override
    public boolean installSnapshot(InstallSnapshotMessage message) {
        int lastIncludeIndex = message.getLastIncludeIndex();
        int lastIncludeTerm = message.getLastIncludeTerm();
        long offset = message.getOffset();
        if (lastIncludeIndex != youngGeneration.getLastIncludeIndex() || lastIncludeTerm != oldGeneration
            .getLastIncludeTerm() || offset != youngGeneration.snapshotSize()) {
            return false;
        }
        youngGeneration.write(message.getChunk());
        if (message.isDone()) {
            List<LogEntry> logEntries = subList(lastIncludeIndex + 1, nextIndex);
            oldGeneration.close();
            youngGeneration.close();
            File file = youngGeneration.rename(lastIncludeIndex, lastIncludeTerm);
            oldGeneration = new OldGeneration(file);
            logEntries.forEach(logEntry -> oldGeneration.pendingLogEntry(logEntry));
            youngGeneration = new YoungGeneration(new File(file.getParentFile(), Generation.INSTALLING_FILE_NAME));
        }
        return true;
    }

    @Override
    public Message createAppendLogEntriesMessage(String leaderId, int term, int nextIndex, int size) {
        int lastIncludeIndex = oldGeneration.getLastIncludeIndex();
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
        int preTerm = oldGeneration.getLastLogTerm();
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
            .done(offset + size >= oldGeneration.snapshotSize())
            .build();
    }

    @Override
    public void advanceCommitIndex(int newCommitIndex, int term) {
        LogEntry entry = getEntry(newCommitIndex);
        if (entry.getTerm() < term || newCommitIndex <= commitIndex) {
            return;
        }
        commitIndex = newCommitIndex;
        oldGeneration.appendLogEntries(commitIndex);
    }
}
