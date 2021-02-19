package cn.ttplatform.lc.constant;

/**
 * @author : wang hao
 * @description : MessageType
 * @date :  2020/8/16 0:29
 **/
public class RpcMessageType {

    private RpcMessageType() {
    }

    /**
     * AppendLogEntriesMessage
     */
    public static final int APPEND_LOG_ENTRIES = 0;

    /**
     * AppendLogEntriesResultMessage
     */
    public static final int APPEND_LOG_ENTRIES_RESULT = 1;

    /**
     * RequestVoteMessage
     */
    public static final int REQUEST_VOTE = 2;

    /**
     * RequestVoteResultMessage
     */
    public static final int REQUEST_VOTE_RESULT = 3;

    /**
     * NodeIdMessage
     */
    public static final int NODE_ID = 4;

    /**
     * PreVoteMessage
     */
    public static final int PRE_VOTE = 5;

    /**
     * PreVoteResultMessage
     */
    public static final int PRE_VOTE_RESULT = 6;

    /**
     * InstallSnapshotMessage
     */
    public static final int INSTALL_SNAPSHOT = 7;

    /**
     * InstallSnapshotResultMessage
     */
    public static final int INSTALL_SNAPSHOT_RESULT = 8;

}
