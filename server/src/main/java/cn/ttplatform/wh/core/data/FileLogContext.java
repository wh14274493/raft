package cn.ttplatform.wh.core.data;

import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.core.GlobalContext;
import cn.ttplatform.wh.core.connector.message.AppendLogEntriesMessage;
import cn.ttplatform.wh.core.connector.message.InstallSnapshotMessage;
import cn.ttplatform.wh.core.data.log.Log;
import cn.ttplatform.wh.core.data.log.LogFile;
import cn.ttplatform.wh.core.data.log.LogIndexFile;
import cn.ttplatform.wh.core.data.snapshot.Snapshot;
import cn.ttplatform.wh.core.data.snapshot.SnapshotBuilder;
import cn.ttplatform.wh.core.data.snapshot.SnapshotFile;
import cn.ttplatform.wh.core.data.tool.PooledByteBuffer;
import cn.ttplatform.wh.core.group.Cluster;
import cn.ttplatform.wh.core.group.Endpoint;
import cn.ttplatform.wh.exception.IncorrectLogIndexNumberException;
import cn.ttplatform.wh.exception.OperateFileException;
import cn.ttplatform.wh.support.Message;
import cn.ttplatform.wh.support.Pool;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Wang Hao
 * @date 2021/2/4 16:55
 */
public class FileLogContext implements LogContext {

    private static final Pattern SNAPSHOT_NAME_PATTERN = Pattern.compile("log-(\\d+)-(\\d+).snapshot");
    private static final Pattern LOG_NAME_PATTERN = Pattern.compile("log-(\\d+)-(\\d+).data");
    private static final Pattern INDEX_NAME_PATTERN = Pattern.compile("log-(\\d+)-(\\d+).index");
    private final Logger logger = LoggerFactory.getLogger(FileLogContext.class);
    private final LinkedList<Log> pending = new LinkedList<>();
    private final SnapshotBuilder snapshotBuilder;
    private final GlobalContext context;
    private LogFile logFile;
    private Snapshot snapshot;
    private LogIndexFile logIndexFile;
    private int commitIndex;
    private int nextIndex;

    public FileLogContext(GlobalContext context) {
        this.context = context;
        ServerProperties properties = context.getProperties();
        File base = properties.getBase();
        Pool<PooledByteBuffer> byteBufferPool = context.getByteBufferPool();
        this.snapshot = new Snapshot(getLatestFile(base, SNAPSHOT_NAME_PATTERN, FileName.EMPTY_SNAPSHOT_FILE_NAME),
            byteBufferPool);
        this.logFile = new LogFile(getLatestFile(base, LOG_NAME_PATTERN, FileName.EMPTY_LOG_FILE_NAME), byteBufferPool);
        this.logIndexFile = new LogIndexFile(getLatestFile(base, INDEX_NAME_PATTERN, FileName.EMPTY_INDEX_FILE_NAME),
            byteBufferPool, snapshot.getLastIncludeIndex());
        this.snapshotBuilder = new SnapshotBuilder(base, byteBufferPool);
        this.commitIndex = logIndexFile.getMaxIndex();
        this.nextIndex = commitIndex + 1;
    }

    private File getLatestFile(File parent, Pattern pattern, String defaultName) {
        File[] files = parent.listFiles();
        if (files == null || files.length == 0) {
            return new File(parent, defaultName);
        }
        Optional<File> fileOptional = Arrays.stream(files)
            .filter(file -> pattern.matcher(file.getName()).matches()).min((o1, o2) -> {
                String o1Name = o1.getName();
                String[] o1Pieces = o1Name.substring(0, o1Name.lastIndexOf('.')).split("-");
                String o2Name = o2.getName();
                String[] o2Pieces = o2Name.substring(0, o1Name.lastIndexOf('.')).split("-");
                return Integer.parseInt(o2Pieces[2]) - Integer.parseInt(o1Pieces[2]);
            });
        return fileOptional.orElse(new File(parent, defaultName));
    }

    @Override
    public int getLastIncludeIndex() {
        return snapshot.getLastIncludeIndex();
    }

    @Override
    public int getLastLogIndex() {
        if (!pending.isEmpty()) {
            return pending.getLast().getIndex();
        }
        if (!logIndexFile.isEmpty()) {
            return logIndexFile.getMaxIndex();
        }
        return snapshot.getLastIncludeIndex();
    }

    @Override
    public int getLastLogTerm() {
        if (!pending.isEmpty()) {
            return pending.getLast().getTerm();
        }
        if (!logIndexFile.isEmpty()) {
            return logIndexFile.getLastLogMetaData().getTerm();
        }
        return snapshot.getLastIncludeTerm();
    }

    @Override
    public boolean isNewerThan(int logIndex, int term) {
        int lastLogTerm = getLastLogTerm();
        return term < lastLogTerm || (term == lastLogTerm && logIndex < getLastLogIndex());
    }

    @Override
    public int getNextIndex() {
        return nextIndex;
    }

    @Override
    public int pendingLog(Log log) {
        log.setIndex(nextIndex);
        if (!pending.isEmpty() && log.getIndex() != pending.getLast().getIndex() + 1) {
            // maybe received an expired message
            throw new IncorrectLogIndexNumberException("The index[" + log.getIndex() + "] number of the log is incorrect ");
        }
        pending.add(log);
        nextIndex++;
        return log.getIndex();
    }

    @Override
    public void pendingLogs(int index, List<Log> entries) {
        removeAfter(index);
        Cluster cluster = context.getCluster();
        if (entries != null && !entries.isEmpty()) {
            for (Log logEntry : entries) {
                if (logEntry.getType() == Log.OLD_NEW) {
                    logger.info("receive a OLD_NEW log[{}] from leader", logEntry);
                    cluster.applyOldNewConfig(logEntry.getCommand());
                    cluster.enterOldNewPhase();
                } else if (logEntry.getType() == Log.NEW) {
                    logger.info("receive a NEW log[{}] from leader", logEntry);
                    cluster.applyNewConfig(logEntry.getCommand());
                    cluster.enterNewPhase();
                }
                if (!pending.isEmpty() && logEntry.getIndex() != pending.getLast().getIndex() + 1) {
                    // maybe received an expired message
                    throw new IncorrectLogIndexNumberException(
                        "The index[" + logEntry.getIndex() + "] number of the log is incorrect ");
                }
                pending.add(logEntry);
            }
            nextIndex = entries.get(entries.size() - 1).getIndex() + 1;
            logger.debug("update nextIndex[{}]", nextIndex);
        }
    }

    /**
     * Remove log entries after {@param index}
     *
     * @param index start index
     */
    private void removeAfter(int index) {
        if (isEmpty() || index >= getLastLogIndex()) {
            return;
        }
        int maxLogIndex = logIndexFile.getMaxIndex();
        if (index <= maxLogIndex) {
            pending.clear();
            logFile.removeAfter(index);
            logIndexFile.removeAfter(index);
        } else {
            while (!pending.isEmpty() && pending.peekLast().getIndex() > index) {
                pending.pollLast();
            }
        }
    }

    @Override
    public boolean checkIndexAndTermIfMatched(int index, int term) {
        logger.debug("checkIndexAndTermIfMatched");
        int lastIncludeIndex = snapshot.getLastIncludeIndex();
        int lastIncludeTerm = snapshot.getLastIncludeTerm();
        if (index < lastIncludeIndex) {
            logger.debug("index[{}] < lastIncludeIndex[{}], unmatched", index, lastIncludeIndex);
            return false;
        }
        if (index == lastIncludeIndex) {
            return term == lastIncludeTerm;
        }
        Log logEntry = getEntry(index);
        if (logEntry == null) {
            logger.debug("not found a log for index[{}].", index);
            return false;
        }
        return term == logEntry.getTerm();
    }


    @Override
    public Message createAppendLogEntriesMessage(String leaderId, int term, Endpoint endpoint, int size) {
        int lastIncludeIndex = snapshot.getLastIncludeIndex();
        int lastIncludeTerm = snapshot.getLastIncludeTerm();
        logger
            .debug("create AppendLogEntriesMessage, lastIncludeIndex:{}, lastIncludeTerm:{}.", lastIncludeIndex, lastIncludeTerm);
        int endpointNextIndex = endpoint.getNextIndex();
        if (endpointNextIndex <= lastIncludeIndex) {
            return null;
        }
        AppendLogEntriesMessage message = AppendLogEntriesMessage.builder()
            .leaderCommitIndex(commitIndex)
            .term(term)
            .matched(endpoint.isMatched())
            .leaderId(leaderId)
            .build();
        if (endpoint.isMatched()) {
            message.setLogEntries(subList(endpointNextIndex, endpointNextIndex + size));
        }
        int preIndex = lastIncludeIndex;
        int preTerm = lastIncludeTerm;
        if (endpointNextIndex - 1 > lastIncludeIndex) {
            Log logEntry = getEntry(endpointNextIndex - 1);
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
            .offset(offset)
            .lastIncludeIndex(snapshot.getLastIncludeIndex())
            .lastIncludeTerm(snapshot.getLastIncludeTerm())
            .chunk(snapshot.read(offset, size))
            .done(offset + size >= snapshot.size())
            .build();
    }

    @Override
    public boolean advanceCommitIndex(int newCommitIndex, int term) {
        if (newCommitIndex <= commitIndex) {
            logger.debug("newCommitIndex[{}]<=commitIndex[{}], can not advance commitIndex", newCommitIndex, commitIndex);
            return false;
        }
        Log log = getEntry(newCommitIndex);
        if (log == null || log.getTerm() < term) {
            logger.debug("find log by newCommitIndex[{}] is {}, unmatched.", newCommitIndex, log);
            return false;
        }
        commitIndex = newCommitIndex;
        logger.debug("update commitIndex to {}", commitIndex);
        int size = commitIndex - logIndexFile.getMaxIndex();
        List<Log> committedLogs = new ArrayList<>(size);
        while (!pending.isEmpty() && pending.peekFirst().getIndex() <= commitIndex) {
            committedLogs.add(pending.pollFirst());
        }
        if (committedLogs.size() == 1) {
            long offset = logFile.append(committedLogs.get(0));
            logIndexFile.append(committedLogs.get(0), offset);
        } else {
            long[] offsets = logFile.append(committedLogs);
            logIndexFile.append(committedLogs, offsets);
        }
        logger.debug("append {} logs", committedLogs.size());
        logger.debug("commit {} success.", commitIndex);
        return true;
    }

    @Override
    public boolean shouldGenerateSnapshot(int snapshotGenerateThreshold) {
        return logFile.size() >= snapshotGenerateThreshold;
    }

    @Override
    public boolean installSnapshot(InstallSnapshotMessage message) {
        int lastIncludeIndex = message.getLastIncludeIndex();
        // if the lastIncludeIndex < oldGeneration.getLastIncludeIndex() means this message is an expired messages.
        // need to prevent repeated consumption of expired messages
        if (lastIncludeIndex <= snapshot.getLastIncludeIndex()) {
            logger.warn("install snapshot message is expired");
            throw new UnsupportedOperationException("the message is expired");
        }
        int lastIncludeTerm = message.getLastIncludeTerm();
        String sourceId = message.getSourceId();
        String snapshotSource = snapshotBuilder.getSnapshotSource();
        long offset = message.getOffset();
        if (offset == 0L) {
            snapshotBuilder.setBaseInfo(lastIncludeIndex, lastIncludeTerm, sourceId);
        }
        long expectedOffset = snapshotBuilder.getInstallOffset();
        if (offset != expectedOffset) {
            logger.warn("the offset[{}] of message is unmatched, expect {}.", offset, expectedOffset);
            throw new UnsupportedOperationException("the offset is unmatched");
        }
        if (!sourceId.equals(snapshotSource)) {
            logger.info("the snapshotSource has changed, receive a message that offset!=0.");
            throw new UnsupportedOperationException("the snapshotSource has changed, receive a message that offset!=0.");
        }
        snapshotBuilder.append(message.getChunk());
        if (message.isDone()) {
            completeBuildingSnapshot(lastIncludeTerm, lastIncludeIndex);
        }
        return true;
    }

    @Override
    public void generateSnapshot(int lastIncludeIndex, byte[] content) {
        int lastIncludeTerm = Optional.ofNullable(logIndexFile.getLogMetaData(lastIncludeIndex))
            .orElseThrow(() ->
                new IncorrectLogIndexNumberException("not found log meta data for index " + lastIncludeIndex + ".")).getTerm();
        snapshotBuilder.setBaseInfo(lastIncludeIndex, lastIncludeTerm, context.getNode().getSelfId());
        Pool<PooledByteBuffer> byteBufferPool = context.getByteBufferPool();
        int size = content.length + SnapshotFile.HEADER_LENGTH;
        PooledByteBuffer byteBuffer = byteBufferPool.allocate(size);
        byteBuffer.putLong(content.length);
        byteBuffer.putInt(lastIncludeIndex);
        byteBuffer.putInt(lastIncludeTerm);
        byteBuffer.put(content);
        snapshotBuilder.append(byteBuffer, size);
        completeBuildingSnapshot(lastIncludeTerm, lastIncludeIndex);
        logger.info("generate snapshot that contains {} bytes lastIncludeIndex is {} and lastIncludeTerm is {}", content.length,
            lastIncludeIndex, lastIncludeTerm);
    }

    private void completeBuildingSnapshot(int lastIncludeTerm, int lastIncludeIndex) {
        context.getExecutor().execute(() -> {
            ServerProperties properties = context.getProperties();
            File base = properties.getBase();
            Pool<PooledByteBuffer> byteBufferPool = context.getByteBufferPool();
            snapshotBuilder.complete();
            this.snapshot.delete();
            this.snapshot = new Snapshot(snapshotBuilder.getFile(), byteBufferPool);
            File logGeneratingFile = new File(base,
                String.format(FileName.LOG_GENERATING_FILE_NAME, lastIncludeTerm, lastIncludeIndex));
            File logIndexGeneratingFile = new File(base,
                String.format(FileName.INDEX_GENERATING_FILE_NAME, lastIncludeTerm, lastIncludeIndex));
            long offset = logIndexFile.getEntryOffset(lastIncludeIndex + 1);
            try {
                if (offset != -1L) {
                    // means that lastIncludeIndex == lastLogIndex
                    this.logFile.transferTo(offset, logGeneratingFile.toPath());
                    this.logIndexFile.transferTo(lastIncludeIndex + 1, logIndexGeneratingFile.toPath());
                }
                this.logFile.delete();
                this.logIndexFile.delete();
                this.logFile = new LogFile(logGeneratingFile, byteBufferPool);
                this.logIndexFile = new LogIndexFile(logIndexGeneratingFile, byteBufferPool, lastIncludeIndex);
            } catch (IOException e) {
                throw new OperateFileException("transfer log error.", e);
            } finally {
                context.getStateMachine().stopGenerating();
            }
        });
    }

    /**
     * Search memory or log in file according to index, and return, if not found, return null
     *
     * @param index index
     * @return Log found
     */
    @Override
    public Log getEntry(int index) {
        if (isEmpty()) {
            logger.debug("young generation is empty.");
            return null;
        }
        long minLogIndex = logIndexFile.getMinIndex();
        if (index < minLogIndex) {
            logger.debug("index[{}] < minEntryIndex[{}]", index, minLogIndex);
            return null;
        }
        int lastLogIndex = getLastLogIndex();
        if (index > lastLogIndex) {
            logger.debug("index[{}] > lastLogIndex[{}]", index, lastLogIndex);
            return null;
        }
        int maxLogIndex = logIndexFile.getMaxIndex();
        if (index > maxLogIndex) {
            logger.debug("index[{}] > maxEntryIndex[{}], find log from pending.", index, maxLogIndex);
            return pending.get(index - maxLogIndex - 1);
        }
        return logFile.getEntry(logIndexFile.getEntryOffset(index), logIndexFile.getEntryOffset(index + 1));
    }

    /**
     * Find the log list from the {@param from} position to the {@param to} position, but this list does not contain the {@param
     * to} position
     *
     * @param from start index
     * @param to   end index
     * @return result list
     */
    @Override
    public List<Log> subList(int from, int to) {
        if (isEmpty()) {
            return Collections.emptyList();
        }
        int lastIndex = getLastLogIndex();
        if (from >= to || from > lastIndex) {
            return Collections.emptyList();
        }
        from = Math.max(logIndexFile.getMinIndex(), from);
        to = Math.min(to, lastIndex + 1);
        logger.debug("sublist from {} to {}", from, to);
        List<Log> res = new ArrayList<>(to - from);
        int maxLogIndex = logIndexFile.getMaxIndex();
        if (from > maxLogIndex) {
            from = from - maxLogIndex - 1;
            to = to - maxLogIndex - 1;
            IntStream.range(from, to).forEach(index -> res.add(pending.get(index)));
            return res;
        }
        long start = logIndexFile.getEntryOffset(from);
        long end;
        if (to <= maxLogIndex) {
            end = logIndexFile.getEntryOffset(to);
        } else {
            end = logFile.size();
        }
        logFile.loadEntriesIntoList(start, end, res);
        to = to - maxLogIndex - 1;
        IntStream.range(0, to).forEach(index -> res.add(pending.get(index)));
        return res;
    }

    @Override
    public PooledByteBuffer getSnapshotData() {
        return snapshot.readAll();
    }

    @Override
    public boolean isEmpty() {
        return pending.isEmpty() && logFile.isEmpty() && logIndexFile.isEmpty();
    }

    @Override
    public void close() {
        logFile.close();
        snapshot.close();
        logIndexFile.close();
    }

}
