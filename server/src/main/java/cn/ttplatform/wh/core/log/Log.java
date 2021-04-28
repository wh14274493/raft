package cn.ttplatform.wh.core.log;


import cn.ttplatform.wh.core.connector.message.InstallSnapshotMessage;
import cn.ttplatform.wh.core.log.entry.LogEntry;
import cn.ttplatform.wh.support.Message;
import java.util.List;

/**
 * @author : wang hao
 * @date :  2020/8/15 23:18
 **/
public interface Log {

    /**
     * get log index of the last log entry.
     *
     * @return return last log index
     */
    int getLastLogIndex();

    /**
     * get log term of the last log entry.
     *
     * @return return last log term
     */
    int getLastLogTerm();

    /**
     * lastLogTerm < context.log().getLastLogTerm() || (lastLogTerm == context.log().getLastLogTerm() && lastLogIndex <
     * context.log().getLastLogIndex()) judge the local log is newer than remote node.
     *
     * @param logIndex last log index of the remote node
     * @param term     last log term of the remote node
     * @return result
     */
    boolean isNewerThan(int logIndex, int term);

    /**
     * get the index of next insert position
     *
     * @return the index of next insert position
     */
    int getNextIndex();

    /**
     * Get entry by index
     *
     * @param index entry index
     * @return the entry
     */
    LogEntry getEntry(int index);

    /**
     * search entry list from {@code from} to {@code to}
     *
     * @param from include from index
     * @param to   exclude to index
     * @return search result
     */
    List<LogEntry> subList(int from, int to);

    /**
     * if {@code index} and {@code term} is matched, then append the {@code entries}, else return false
     *
     * @param index   log index
     * @param entries entry list to be appended
     */
    void pendingEntries(int index, List<LogEntry> entries);

    boolean checkIndexAndTermIfMatched(int index, int term);

    /**
     * install snapshot file that content from leader
     *
     * @param message InstallSnapshotMessage
     * @return install result
     */
    boolean installSnapshot(InstallSnapshotMessage message);

    /**
     * create an AppendLogEntriesMessage with the nextIndex of a endpoint replication state
     *
     * @param leaderId  self id
     * @param term      current term
     * @param nextIndex the first log index in log entries
     * @param size      log entries size in message
     * @return an AppendLogEntriesMessage
     */
    Message createAppendLogEntriesMessage(String leaderId, int term, int nextIndex, int size);

    /**
     * create an InstallSnapshotMessage with the offset of a endpoint replication state
     *
     * @param term   current term
     * @param offset the offset of a endpoint replication state
     * @param size   The size of the transfer log
     * @return an InstallSnapshotMessage
     */
    InstallSnapshotMessage createInstallSnapshotMessage(int term, long offset, int size);

    /**
     * All logs with index less than commitIndex need to be committed.
     *
     * @param commitIndex commitIndex
     * @param term        current term
     * @return committed res
     */
    boolean advanceCommitIndex(int commitIndex, int term);

    /**
     * Determine whether need to generate a log snapshot
     *
     * @param snapshotGenerateThreshold snapshotGenerateThreshold
     * @return res
     */
    boolean shouldGenerateSnapshot(int snapshotGenerateThreshold);

    /**
     * Generate log snapshots based on state machine data
     *
     * @param lastIncludeIndex the last log index be included in snapshot
     * @param content          state machine data
     */
    void generateSnapshot(int lastIncludeIndex, byte[] content);

    /**
     * pending an log entry
     *
     * @param logEntry log entry
     */
    void pendingEntry(LogEntry logEntry);

    /**
     * load snapshot snapshot date into state machine
     *
     * @return snapshot
     */
    byte[] getSnapshotData();

    /**
     * the last log index be included in snapshot
     *
     * @return lastIncludeIndex
     */
    int getLastIncludeIndex();

    void close();
}
