package cn.ttplatform.wh.data;

import static cn.ttplatform.wh.data.FileConstant.EMPTY_SNAPSHOT_FILE_NAME;
import static cn.ttplatform.wh.data.FileConstant.INDEX_NAME_SUFFIX;
import static cn.ttplatform.wh.data.FileConstant.LOG_NAME_SUFFIX;
import static cn.ttplatform.wh.data.FileConstant.SNAPSHOT_NAME_PATTERN;
import static cn.ttplatform.wh.data.FileConstant.SNAPSHOT_NAME_SUFFIX;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Wang Hao
 * @date 2021/2/4 16:55
 */
public class LogManager {

    private final Logger logger = LoggerFactory.getLogger(LogManager.class);
    private final LinkedList<Log> pending = new LinkedList<>();
    private final Pool<PooledByteBuffer> byteBufferPool;
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
        this.byteBufferPool = context.getByteBufferPool();
        File[] files = base.listFiles();
        File latestSnapshotFile = getLatestFile(base, files);
        this.snapshot = new Snapshot(latestSnapshotFile, byteBufferPool);
        String path = latestSnapshotFile.getPath();
        File latestLogFile = new File(path.replace(SNAPSHOT_NAME_SUFFIX, LOG_NAME_SUFFIX));
        this.logFile = new LogFile(latestLogFile, byteBufferPool);
        File latestLogIndexFile = new File(path.replace(SNAPSHOT_NAME_SUFFIX, INDEX_NAME_SUFFIX));
        this.logIndexFile = new LogIndexFile(latestLogIndexFile, byteBufferPool, snapshot.getLastIncludeIndex());
        if (logIndexFile.isEmpty()) {
            // means that the index file is wrong or missing.
            rebuildIndexFile();
        }
        this.snapshotBuilder = new SnapshotBuilder(base, byteBufferPool);
        this.commitIndex = logIndexFile.getMaxIndex();
        this.nextIndex = commitIndex + 1;
    }

    private File getLatestFile(File parent, File[] files) {
        if (files == null || files.length == 0) {
            return new File(parent, EMPTY_SNAPSHOT_FILE_NAME);
        }
        Optional<File> fileOptional = Arrays.stream(files)
            .filter(file -> SNAPSHOT_NAME_PATTERN.matcher(file.getName()).matches()).min((o1, o2) -> {
                String o1Name = o1.getName();
                String[] o1Pieces = o1Name.substring(0, o1Name.lastIndexOf('.')).split("-");
                String o2Name = o2.getName();
                String[] o2Pieces = o2Name.substring(0, o1Name.lastIndexOf('.')).split("-");
                return Integer.parseInt(o2Pieces[2]) - Integer.parseInt(o1Pieces[2]);
            });
        return fileOptional.orElse(new File(parent, EMPTY_SNAPSHOT_FILE_NAME));
    }

    /**
     * Rebuild the index file based on the log file
     */
    private void rebuildIndexFile() {
        logger.info("rebuild index file.");
        PooledByteBuffer byteBuffer = logFile.loadAll();
        // allocate a buffer that size is 10MB. And this buffer can include 524288 index records.
        PooledByteBuffer buffer = byteBufferPool.allocate(10 * 1024 * 1024);
        try {
            long offset = 0L;
            long position = 0L;
            while (byteBuffer.hasRemaining()) {
                while (byteBuffer.hasRemaining() && buffer.hasRemaining()) {
                    buffer.putInt(byteBuffer.getInt());
                    buffer.putInt(byteBuffer.getInt());
                    buffer.putInt(byteBuffer.getInt());
                    buffer.putLong(offset);
                    offset = offset + 16 + byteBuffer.getInt();
                    byteBuffer.position((int) offset);
                }
                int lastIndex = buffer.position();
                logIndexFile.append(buffer);
                position += lastIndex;
            }
        } finally {
            byteBufferPool.recycle(byteBuffer);
            byteBufferPool.recycle(buffer);
        }
        logIndexFile.initialize();
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
            return pending.getLast().getTerm();
        }
        if (!logIndexFile.isEmpty()) {
            return logIndexFile.getLastLogMetaData().getTerm();
        }
        return snapshot.getLastIncludeTerm();
    }

    public int getTermOfLog(int index) {
        return Optional.ofNullable(logIndexFile.getLogMetaData(index))
            .orElseThrow(() -> new IncorrectLogIndexNumberException("not found log meta data for index " + index + "."))
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
        if (!pending.isEmpty() && log.getIndex() != pending.getLast().getIndex() + 1) {
            // maybe received an expired message
            throw new IncorrectLogIndexNumberException("The index[" + log.getIndex() + "] number of the log is incorrect ");
        }
        pending.add(log);
        nextIndex++;
        return log.getIndex();
    }

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
            while (!pending.isEmpty() && pending.peekLast().getIndex() > index) {
                pending.pollLast();
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
            completeBuildingSnapshot(snapshotBuilder, lastIncludeTerm, lastIncludeIndex);
        }
        return true;
    }

    public void completeBuildingSnapshot(SnapshotBuilder snapshotBuilder, int lastIncludeTerm, int lastIncludeIndex) {
        context.getExecutor().execute(() -> {
            snapshotBuilder.complete();
            this.snapshot = new Snapshot(snapshotBuilder.getFile(), byteBufferPool);
            File logGeneratingFile = new File(base,
                String.format(FileConstant.LOG_GENERATING_FILE_NAME, lastIncludeTerm, lastIncludeIndex));
            File logIndexGeneratingFile = new File(base,
                String.format(FileConstant.INDEX_GENERATING_FILE_NAME, lastIncludeTerm, lastIncludeIndex));
            long offset = logIndexFile.getEntryOffset(lastIncludeIndex + 1);
            try {
                if (offset != -1L) {
                    // means that lastIncludeIndex == lastLogIndex
                    this.logFile.transferTo(offset, logGeneratingFile.toPath());
                }
                this.logFile = new LogFile(logGeneratingFile, byteBufferPool);
                this.logIndexFile = new LogIndexFile(logIndexGeneratingFile, byteBufferPool, lastIncludeIndex);
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
