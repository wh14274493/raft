package cn.ttplatform.wh.core.log;


import cn.ttplatform.wh.core.log.entry.LogEntry;
import cn.ttplatform.wh.core.connector.message.InstallSnapshotMessage;
import cn.ttplatform.wh.core.connector.message.Message;
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
     * get commit index for Leader role
     *
     * @return commit index
     */
    int getCommitIndex();

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
     * search entry list from {@code from} to nextIndex if to-from > maxLength, then return the entry list that index in
     * [from, from + maxLength) else return the entry list that index in [from, nextIndex)
     *
     * @param from      include from index
     * @param maxLength max size of the result list
     * @return search result
     */
    List<LogEntry> subListWithMaxLength(int from, int maxLength);

    /**
     * if {@code index} and {@code term} is matched, then append the {@code entries}, else return false
     *
     * @param index   log index
     * @param term    log term
     * @param entries entry list to be appended
     * @return append result
     */
    boolean appendEntries(int index, int term, List<LogEntry> entries);

    /**
     * install snapshot file that content from leader
     *
     * @return install result
     */
    boolean installSnapshot(InstallSnapshotMessage message);

    Message createAppendLogEntriesMessage(String leaderId, int term, int nextIndex, int size);

    Message createInstallSnapshotMessage(int term, long offset, int size);

    List<LogEntry> advanceCommitIndex(int commitIndex, int term);

    boolean shouldGenerateSnapshot(int snapshotGenerateThreshold);

    void generateSnapshot(int lastIncludeIndex, int lastIncludeTerm, byte[] content);

    void appendEntry(LogEntry logEntry);

    byte[] getSnapshotData();

    int getLastIncludeIndex();
}
