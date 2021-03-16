package cn.ttplatform.wh.core.log;

import cn.ttplatform.wh.constant.FileName;
import cn.ttplatform.wh.domain.entry.LogEntry;
import cn.ttplatform.wh.domain.message.AppendLogEntriesMessage;
import cn.ttplatform.wh.domain.message.InstallSnapshotMessage;
import cn.ttplatform.wh.domain.message.Message;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * @author Wang Hao
 * @date 2021/2/4 16:55
 */
public class FileLog implements Log {

    private static final Pattern DIR_NAME_PATTERN = Pattern.compile("log-(\\d+)");
    private YoungGeneration generatingGeneration;
    private YoungGeneration installingGeneration;
    private OldGeneration oldGeneration;
    private int commitIndex;
    private int nextIndex;

    public FileLog(File parent) {
        generatingGeneration = new YoungGeneration(new File(parent, FileName.GENERATING_FILE_NAME));
        installingGeneration = new YoungGeneration(new File(parent, FileName.INSTALLING_FILE_NAME));
        oldGeneration = new OldGeneration(getLatestGeneration(parent));
        commitIndex = oldGeneration.getMaxLogIndex();
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
        appendEntries(entries);
        return true;
    }

    private void appendEntries(List<LogEntry> entries) {
        entries.forEach(entry -> oldGeneration.pendingLogEntry(entry));
        updateNextIndex();
    }

    @Override
    public void appendEntry(LogEntry entry) {
        oldGeneration.pendingLogEntry(entry);
        updateNextIndex();
    }

    private void updateNextIndex() {
        this.nextIndex = oldGeneration.getLastLogIndex() + 1;
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
    public List<LogEntry> advanceCommitIndex(int newCommitIndex, int term) {
        LogEntry entry = getEntry(newCommitIndex);
        if (entry.getTerm() < term || newCommitIndex <= commitIndex) {
            return Collections.emptyList();
        }
        commitIndex = newCommitIndex;
        return oldGeneration.commit(commitIndex);
    }

    @Override
    public boolean shouldGenerateSnapshot(int snapshotGenerateThreshold) {
        return oldGeneration.getEntryFileSize() >= snapshotGenerateThreshold;
    }

    @Override
    public boolean installSnapshot(InstallSnapshotMessage message) {
        int lastIncludeIndex = message.getLastIncludeIndex();
        int lastIncludeTerm = message.getLastIncludeTerm();
        long offset = message.getOffset();
        if (lastIncludeIndex != installingGeneration.getLastIncludeIndex() || lastIncludeTerm != oldGeneration
            .getLastIncludeTerm() || offset != installingGeneration.snapshotSize()) {
            return false;
        }
        installingGeneration.write(message.getChunk());
        if (message.isDone()) {
            List<LogEntry> logEntries = subList(lastIncludeIndex + 1, nextIndex);
            oldGeneration.close();
            installingGeneration.close();
            File file = installingGeneration.rename(lastIncludeIndex, lastIncludeTerm);
            oldGeneration = new OldGeneration(file);
            appendEntries(logEntries);
            installingGeneration = new YoungGeneration(new File(file.getParentFile(), FileName.INSTALLING_FILE_NAME));
        }
        return true;
    }

    @Override
    public void generateSnapshot(int lastIncludeIndex, int lastIncludeTerm, byte[] content) {
        generatingGeneration.generateSnapshot(lastIncludeIndex, lastIncludeTerm, content);
        List<LogEntry> logEntries = subList(lastIncludeIndex + 1, nextIndex);
        oldGeneration.close();
        generatingGeneration.close();
        File file = generatingGeneration.rename(lastIncludeIndex, lastIncludeTerm);
        oldGeneration = new OldGeneration(file);
        appendEntries(logEntries);
        generatingGeneration = new YoungGeneration(new File(file.getParentFile(), FileName.INSTALLING_FILE_NAME));
    }
}
