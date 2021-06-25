package cn.ttplatform.wh.data;

import cn.ttplatform.wh.GlobalContext;
import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.data.log.Log;
import cn.ttplatform.wh.data.log.LogFile;
import cn.ttplatform.wh.data.log.LogIndexFile;
import cn.ttplatform.wh.data.snapshot.Snapshot;
import cn.ttplatform.wh.data.snapshot.SnapshotBuilder;
import cn.ttplatform.wh.data.tool.PooledByteBuffer;
import cn.ttplatform.wh.exception.IncorrectLogIndexNumberException;
import cn.ttplatform.wh.exception.OperateFileException;
import cn.ttplatform.wh.group.Cluster;
import cn.ttplatform.wh.group.Endpoint;
import cn.ttplatform.wh.message.AppendLogEntriesMessage;
import cn.ttplatform.wh.message.InstallSnapshotMessage;
import cn.ttplatform.wh.support.Message;
import cn.ttplatform.wh.support.Pool;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Wang Hao
 * @date 2021/2/4 16:55
 */
public class LogManager {

    private final Logger logger = LoggerFactory.getLogger(LogManager.class);
    private final TreeMap<Integer, Log> pending = new TreeMap<>();
    private final Pool<PooledByteBuffer> bufferPool;
    private final SnapshotBuilder snapshotBuilder;
    private final GlobalContext context;
    private final File base;
    private LogFile logFile;
    private Snapshot snapshot;
    private LogIndexFile logIndexFile;
    private int commitIndex;
    private int nextIndex;

    public LogManager(GlobalContext context) {
        this.context = context;
        ServerProperties properties = context.getProperties();
        this.base = properties.getBase();
        this.bufferPool = context.getByteBufferPool();
        File latestSnapshotFile = FileConstant.getLatestSnapshotFile(base);
        this.snapshot = new Snapshot(latestSnapshotFile, bufferPool);
        String path = latestSnapshotFile.getPath();
        this.logFile = new LogFile(FileConstant.getMatchedLogFile(path), bufferPool);
        File latestLogIndexFile = FileConstant.getMatchedLogIndexFile(path);
        this.logIndexFile = new LogIndexFile(latestLogIndexFile, bufferPool, snapshot.getLastIncludeIndex());
        if (!logFile.isEmpty() && logIndexFile.isEmpty()) {
            // means that the index file is wrong or missing.
            rebuildIndexFile();
        }
        this.snapshotBuilder = new SnapshotBuilder(base, bufferPool);
        this.commitIndex = logIndexFile.getMaxIndex();
        this.nextIndex = commitIndex + 1;
    }


    /**
     * Rebuild the index file based on the log file
     */
    private void rebuildIndexFile() {
        PooledByteBuffer byteBuffer = logFile.loadAll();
        // allocate a buffer that size is 10MB. And this buffer can include 524288 index records.
        PooledByteBuffer buffer = bufferPool.allocate(10 * 1024 * 1024);
        try {
            long offset = 0L;
            while (byteBuffer.hasRemaining()) {
                while (byteBuffer.hasRemaining() && buffer.hasRemaining()) {
                    buffer.putInt(byteBuffer.getInt());
                    buffer.putInt(byteBuffer.getInt());
                    buffer.putInt(byteBuffer.getInt());
                    buffer.putLong(offset);
                    offset = offset + 16 + byteBuffer.getInt();
                    byteBuffer.position((int) offset);
                }
                logIndexFile.append(buffer);
            }
        } finally {
            bufferPool.recycle(byteBuffer);
            bufferPool.recycle(buffer);
        }
        logIndexFile.initialize();
        logger.info("rebuild index file success.");
    }

    public int getNextIndex() {
        return nextIndex;
    }

    public int getLastIncludeIndex() {
        return snapshot.getLastIncludeIndex();
    }

    public int getIndexOfLastLog() {
        return nextIndex - 1;
    }

    public int getTermOfLastLog() {
        if (!pending.isEmpty()) {
            return pending.lastEntry().getValue().getTerm();
        }
        if (!logIndexFile.isEmpty()) {
            return logIndexFile.getLastLogMetaData().getTerm();
        }
        return snapshot.getLastIncludeTerm();
    }

    public int getTermOfLog(int index) {
        Log log = pending.get(index);
        if (log != null) {
            return log.getTerm();
        }
        return Optional.ofNullable(logIndexFile.getLogMetaData(index))
            .orElseThrow(() -> new IncorrectLogIndexNumberException("not found log meta data for index[" + index + "]."))
            .getTerm();
    }

    /**
     * lastLogTerm < context.log().getLastLogTerm() || (lastLogTerm == context.log().getLastLogTerm() && lastLogIndex <
     * context.log().getLastLogIndex()) judge the local log is newer than remote node.
     *
     * @param logIndex last log index of the remote node
     * @param term     last log term of the remote node
     * @return result
     */
    public boolean isNewerThan(int logIndex, int term) {
        int lastLogTerm = getTermOfLastLog();
        return term < lastLogTerm || (term == lastLogTerm && logIndex < getIndexOfLastLog());
    }

    public int pendingLog(Log log) {
        log.setIndex(nextIndex);
        if (!pending.isEmpty() && log.getIndex() != pending.lastEntry().getKey() + 1) {
            // maybe received an expired message
            throw new IncorrectLogIndexNumberException("The index[" + log.getIndex() + "] number of the log is incorrect ");
        }
        pending.put(log.getIndex(), log);
        nextIndex++;
        return log.getIndex();
    }

    public void pendingLogs(int index, List<Log> entries) {
        removeAfter(index);
        Cluster cluster = context.getCluster();
        if (entries != null && !entries.isEmpty()) {
            for (Log log : entries) {
                if (log.getType() == Log.OLD_NEW) {
                    logger.info("receive a OLD_NEW log[{}] from leader", log);
                    cluster.applyOldNewConfig(log.getCommand());
                    cluster.enterOldNewPhase();
                } else if (log.getType() == Log.NEW) {
                    logger.info("receive a NEW log[{}] from leader", log);
                    cluster.applyNewConfig(log.getCommand());
                    cluster.enterNewPhase();
                }
                if (!pending.isEmpty() && log.getIndex() != pending.lastEntry().getKey() + 1) {
                    // maybe received an expired message
                    throw new IncorrectLogIndexNumberException(
                        "The index[" + log.getIndex() + "] number of the log is incorrect.");
                }
                pending.put(log.getIndex(), log);
            }
            nextIndex = entries.get(entries.size() - 1).getIndex() + 1;
            logger.debug("update nextIndex[{}]", nextIndex);
        }
    }

    private void removeAfter(int index) {
        if (isEmpty() || index >= getIndexOfLastLog()) {
            return;
        }
        int maxLogIndex = logIndexFile.getMaxIndex();
        if (index <= maxLogIndex) {
            pending.clear();
            logFile.removeAfter(index);
            logIndexFile.removeAfter(index);
        } else {
            while (!pending.isEmpty() && pending.lastEntry().getKey() > index) {
                pending.pollLastEntry();
            }
        }
    }

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
        Log logEntry = getLog(index);
        if (logEntry == null) {
            logger.debug("not found a log for index[{}].", index);
            return false;
        }
        return term == logEntry.getTerm();
    }

    public Message createAppendLogEntriesMessage(String leaderId, int term, Endpoint endpoint, int size) {
        int lastIncludeIndex = snapshot.getLastIncludeIndex();
        int lastIncludeTerm = snapshot.getLastIncludeTerm();
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
            message.setLogEntries(range(endpointNextIndex, endpointNextIndex + size));
        }
        int preIndex = lastIncludeIndex;
        int preTerm = lastIncludeTerm;
        if (endpointNextIndex - 1 > lastIncludeIndex) {
            Log logEntry = getLog(endpointNextIndex - 1);
            preIndex = logEntry.getIndex();
            preTerm = logEntry.getTerm();
        }
        message.setPreLogIndex(preIndex);
        message.setPreLogTerm(preTerm);
        return message;
    }

    public Message createInstallSnapshotMessage(int term, long offset, int size) {
        return InstallSnapshotMessage.builder().term(term)
            .offset(offset)
            .lastIncludeIndex(snapshot.getLastIncludeIndex())
            .lastIncludeTerm(snapshot.getLastIncludeTerm())
            .chunk(snapshot.read(offset, size))
            .done(offset + size >= snapshot.size())
            .build();
    }

    /**
     * All logs with index less than commitIndex need to be committed.
     *
     * @param newCommitIndex commitIndex
     * @param term           current term
     * @return committed res
     */
    public boolean advanceCommitIndex(int newCommitIndex, int term) {
        if (newCommitIndex <= commitIndex) {
            logger.debug("newCommitIndex[{}]<=commitIndex[{}], can not advance commitIndex", newCommitIndex, commitIndex);
            return false;
        }
        Log log = getLog(newCommitIndex);
        if (log == null || log.getTerm() < term) {
            logger.debug("find log by newCommitIndex[{}] is {}, unmatched.", newCommitIndex, log);
            return false;
        }
        int size = newCommitIndex - logIndexFile.getMaxIndex();
        List<Log> committedLogs = new ArrayList<>(size);
        while (!pending.isEmpty() && pending.firstEntry().getKey() <= newCommitIndex) {
            committedLogs.add(pending.pollFirstEntry().getValue());
        }
        if (committedLogs.size() == 1) {
            long offset = logFile.append(committedLogs.get(0));
            logIndexFile.append(committedLogs.get(0), offset);
        } else {
            long[] offsets = logFile.append(committedLogs);
            logIndexFile.append(committedLogs, offsets);
        }
        commitIndex = newCommitIndex;
        if (logger.isDebugEnabled()) {
            logger.debug("append {} logs", committedLogs.size());
            logger.debug("commit {} success.", commitIndex);
        }
        return true;
    }

    /**
     * Determine whether need to generate a log snapshot
     *
     * @param snapshotGenerateThreshold snapshotGenerateThreshold
     * @return res
     */
    public boolean shouldGenerateSnapshot(int snapshotGenerateThreshold) {
        return logFile.size() >= snapshotGenerateThreshold;
    }

    /**
     * install snapshot file that content from leader
     *
     * @param message InstallSnapshotMessage
     * @return install result
     */
    public boolean installSnapshot(InstallSnapshotMessage message) {
        int lastIncludeIndex = message.getLastIncludeIndex();
        // if the lastIncludeIndex < oldGeneration.getLastIncludeIndex() means this message is an expired messages.
        // need to prevent repeated consumption of expired messages
        if (lastIncludeIndex <= snapshot.getLastIncludeIndex()) {
            throw new UnsupportedOperationException("the InstallSnapshotMessage is expired");
        }
        int lastIncludeTerm = message.getLastIncludeTerm();
        String sourceId = message.getSourceId();
        long offset = message.getOffset();
        if (offset == 0L) {
            snapshotBuilder.setBaseInfo(lastIncludeIndex, lastIncludeTerm, sourceId);
        }
        long expectedOffset = snapshotBuilder.getInstallOffset();
        if (offset != expectedOffset) {
            throw new IllegalArgumentException(String.format("the offset[%d] is unmatched. expect %d", offset, expectedOffset));
        }
        if (!sourceId.equals(snapshotBuilder.getSnapshotSource())) {
            throw new IllegalArgumentException("the snapshotSource has changed, receive a message that offset!=0.");
        }
        snapshotBuilder.append(message.getChunk());
        if (message.isDone()) {
            completeBuildingSnapshot(snapshotBuilder, lastIncludeTerm, lastIncludeIndex);
        }
        return true;
    }

    public void completeBuildingSnapshot(SnapshotBuilder snapshotBuilder, int lastIncludeTerm, int lastIncludeIndex) {
        context.getExecutor().execute(() -> {
            snapshotBuilder.complete();
            this.snapshot = new Snapshot(snapshotBuilder.getFile(), bufferPool);
            File newLogFile = FileConstant.newLogFile(base, lastIncludeIndex, lastIncludeTerm);
            File newLogIndexFile = FileConstant.newLogIndexFile(base, lastIncludeIndex, lastIncludeTerm);
            long offset = logIndexFile.getEntryOffset(lastIncludeIndex + 1);
            try {
                if (offset != -1L) {
                    // means that lastIncludeIndex == lastLogIndex
                    this.logFile.transferTo(offset, newLogFile.toPath());
                }
                this.logFile = new LogFile(newLogFile, bufferPool);
                this.logIndexFile = new LogIndexFile(newLogIndexFile, bufferPool, lastIncludeIndex);
                rebuildIndexFile();
            } catch (IOException e) {
                throw new OperateFileException("transfer log error.", e);
            } finally {
                context.getStateMachine().stopGenerating();
            }
        });
    }

    public Log getLog(int index) {
        if (isEmpty()) {
            logger.debug("young generation is empty.");
            return null;
        }
        long minLogIndex = logIndexFile.getMinIndex();
        if (index < minLogIndex) {
            logger.debug("index[{}] < minEntryIndex[{}]", index, minLogIndex);
            return null;
        }
        int lastLogIndex = getIndexOfLastLog();
        if (index > lastLogIndex) {
            logger.debug("index[{}] > lastLogIndex[{}]", index, lastLogIndex);
            return null;
        }
        int maxLogIndex = logIndexFile.getMaxIndex();
        if (index > maxLogIndex) {
            logger.debug("index[{}] > maxEntryIndex[{}], find log from pending.", index, maxLogIndex);
            return pending.get(index);
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
    public List<Log> range(int from, int to) {
        if (isEmpty()) {
            return Collections.emptyList();
        }
        int lastIndex = getIndexOfLastLog();
        if (from >= to || from > lastIndex) {
            return Collections.emptyList();
        }
        from = Math.max(logIndexFile.getMinIndex(), from);
        to = Math.min(to, lastIndex + 1);
        logger.debug("sublist from {} to {}", from, to);
        List<Log> res = new ArrayList<>(to - from);
        int maxLogIndex = logIndexFile.getMaxIndex();
        if (from > maxLogIndex) {
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
        from = maxLogIndex + 1;
        IntStream.range(from, to).forEach(index -> res.add(pending.get(index)));
        return res;
    }

    public PooledByteBuffer getSnapshotData() {
        return snapshot.readAll();
    }

    public boolean isEmpty() {
        return pending.isEmpty() && logFile.isEmpty() && logIndexFile.isEmpty();
    }

    public void close() {
        logFile.close();
        snapshot.close();
        logIndexFile.close();
    }

}