package cn.ttplatform.wh.core.log;

import cn.ttplatform.wh.core.GlobalContext;
import cn.ttplatform.wh.core.connector.message.AppendLogEntriesMessage;
import cn.ttplatform.wh.core.connector.message.InstallSnapshotMessage;
import cn.ttplatform.wh.core.group.Cluster;
import cn.ttplatform.wh.core.log.entry.LogEntry;
import cn.ttplatform.wh.core.log.generation.FileName;
import cn.ttplatform.wh.core.log.generation.OldGeneration;
import cn.ttplatform.wh.core.log.generation.YoungGeneration;
import cn.ttplatform.wh.support.Message;
import java.io.File;
import java.util.Arrays;
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
    private final GlobalContext context;
    private YoungGeneration youngGeneration;
    private OldGeneration oldGeneration;
    private int commitIndex;
    private int nextIndex;

    public FileLog(GlobalContext context) {
        this.context = context;
        oldGeneration = new OldGeneration(getLatestGeneration(context.getBasePath()), context.getByteBufferPool());
        youngGeneration = new YoungGeneration(context.getBasePath(), context.getByteBufferPool(),
            oldGeneration.getLastIncludeIndex());
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
    public void pendingEntries(int index, List<LogEntry> entries) {
        youngGeneration.removeAfter(index);
        Cluster cluster = context.getCluster();
        if (entries != null && !entries.isEmpty()) {
            entries.forEach(logEntry -> {
                if (logEntry.getType() == LogEntry.OLD_NEW) {
                    log.info("receive a OLD_NEW log[{}] from leader", logEntry);
                    cluster.applyOldNewConfig(logEntry.getCommand());
                    cluster.enterOldNewPhase();
                } else if (logEntry.getType() == LogEntry.NEW) {
                    log.info("receive a NEW log[{}] from leader", logEntry);
                    cluster.applyNewConfig(logEntry.getCommand());
                    cluster.enterNewPhase();
                }
                youngGeneration.pendingEntry(logEntry);
            });
            nextIndex = entries.get(entries.size() - 1).getIndex() + 1;
            log.debug("update nextIndex[{}]", nextIndex);
        }
    }

    @Override
    public boolean checkIndexAndTermIfMatched(int index, int term) {
        log.debug("checkIndexAndTermIfMatched");
        int lastIncludeIndex = oldGeneration.getLastIncludeIndex();
        int lastIncludeTerm = oldGeneration.getLastIncludeTerm();
        if (index < lastIncludeIndex) {
            log.debug("index[{}] < lastIncludeIndex[{}], unmatched", index, lastIncludeIndex);
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

    @Override
    public int pendingEntry(LogEntry entry) {
        entry.setIndex(nextIndex++);
        youngGeneration.pendingEntry(entry);
        log.debug("update nextIndex[{}]", nextIndex);
        return entry.getIndex();
    }

    @Override
    public Message createAppendLogEntriesMessage(String leaderId, int term, int nextIndex, int size) {
        log.debug("create AppendLogEntriesMessage.");
        int lastIncludeIndex = oldGeneration.getLastIncludeIndex();
        int lastIncludeTerm = oldGeneration.getLastIncludeTerm();
        if (nextIndex <= lastIncludeIndex) {
            return null;
        }
        AppendLogEntriesMessage message = AppendLogEntriesMessage.builder()
            .leaderCommitIndex(commitIndex)
            .term(term)
            .leaderId(leaderId)
            .logEntries(subList(nextIndex, nextIndex + size))
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
    public InstallSnapshotMessage createInstallSnapshotMessage(int term, long offset, int size) {
        return InstallSnapshotMessage.builder().term(term)
            .lastIncludeIndex(oldGeneration.getLastIncludeIndex())
            .lastIncludeTerm(oldGeneration.getLastIncludeTerm())
            .chunk(oldGeneration.readSnapshot(offset, size))
            .done(offset + size >= oldGeneration.getSnapshotSize())
            .build();
    }

    @Override
    public boolean advanceCommitIndex(int newCommitIndex, int term) {
        if (newCommitIndex <= commitIndex) {
            log.debug("newCommitIndex[{}]<=commitIndex[{}], can not advance commitIndex", newCommitIndex, commitIndex);
            return false;
        }
        LogEntry entry = getEntry(newCommitIndex);
        if (entry == null || entry.getTerm() < term) {
            log.debug("find entry by newCommitIndex[{}] is {}, unmatched.", newCommitIndex, entry);
            return false;
        }
        commitIndex = newCommitIndex;
        log.debug("update commitIndex to {}", commitIndex);
        youngGeneration.commit(commitIndex);
        log.debug("commit {} success.", commitIndex);
        return true;
    }

    @Override
    public boolean shouldGenerateSnapshot(int snapshotGenerateThreshold) {
        return youngGeneration.getLogEntryFileSize() >= snapshotGenerateThreshold;
    }

    @Override
    public boolean installSnapshot(InstallSnapshotMessage message) {
        int lastIncludeIndex = message.getLastIncludeIndex();
        // if the lastIncludeIndex < oldGeneration.getLastIncludeIndex() means this message is an expired messages.
        // need to prevent repeated consumption of expired messages
        if (lastIncludeIndex <= oldGeneration.getLastIncludeIndex()) {
            log.warn("install snapshot message is expired");
            throw new UnsupportedOperationException("the message is expired");
        }
        int lastIncludeTerm = message.getLastIncludeTerm();
        String messageSourceId = message.getSourceId();
        String snapshotSource = youngGeneration.getSnapshotSource();
        long offset = message.getOffset();
        if (lastIncludeIndex != youngGeneration.getLastIncludeIndex()
            || lastIncludeTerm != youngGeneration.getLastIncludeTerm()) {
            log.warn("lastIncludeIndex[{}] and lastIncludeTerm[{}] is unexpect.", lastIncludeIndex, lastIncludeTerm);
            return false;
        }
        if (!messageSourceId.equals(snapshotSource) && offset != 0L) {
            // The leader has changed and needs to start transmission from offset==0 again
            log.warn("The snapshotSource has changed and needs to start transmission from offset==0 again");
            return false;
        }
        long expectedOffset = youngGeneration.getSnapshotSize();
        if (messageSourceId.equals(snapshotSource) && offset != expectedOffset) {
            log.warn("the offset[{}] of message is unmatched, expect {}.", offset, expectedOffset);
            throw new UnsupportedOperationException("the offset is unmatched");
        }
        if (!messageSourceId.equals(snapshotSource)) {
            log.info("the snapshotSource has changed, reset install snapshot task state");
            // The leader has changed, and the file needs to be cleared before restarting the transfer of the log snapshot
            youngGeneration.clearSnapshot();
            // Update log snapshot source
            youngGeneration.setSnapshotSource(messageSourceId);
        }
        youngGeneration.writeSnapshot(message.getChunk());
        if (message.isDone()) {
            replace(lastIncludeIndex, lastIncludeTerm);
        }
        return true;
    }

    @Override
    public void generateSnapshot(int lastIncludeIndex, byte[] content) {
        int lastIncludeTerm = youngGeneration.getEntryMetaData(lastIncludeIndex).getTerm();
        youngGeneration.generateSnapshot(lastIncludeIndex, lastIncludeTerm, content);
        log.info("generate snapshot that lastIncludeIndex is {} and lastIncludeTerm is {}", lastIncludeIndex, lastIncludeTerm);
        replace(lastIncludeIndex, lastIncludeTerm);
    }

    private void replace(int lastIncludeIndex, int lastIncludeTerm) {
        List<LogEntry> logEntries = subList(lastIncludeIndex + 1, nextIndex);
        oldGeneration.close();
        youngGeneration.close();
        File file = youngGeneration.rename(lastIncludeIndex, lastIncludeTerm);
        oldGeneration = new OldGeneration(file, context.getByteBufferPool());
        log.info("The new generation successfully promoted to the old generation and re-created the new generation ");
        youngGeneration = new YoungGeneration(file.getParentFile(), context.getByteBufferPool(),
            oldGeneration.getLastIncludeIndex());
        youngGeneration.appendLogEntries(logEntries);
    }

    @Override
    public byte[] getSnapshotData() {
        return oldGeneration.readSnapshot();
    }

    @Override
    public void close() {
        youngGeneration.close();
        oldGeneration.close();
    }

}
