package cn.ttplatform.wh.constant;

/**
 * @author Wang Hao
 * @date 2021/4/29 0:32
 */
public class DistributableType {

    private DistributableType(){}

    public static final int SET_COMMAND = 0;
    public static final int SET_COMMAND_RESULT = 1;
    public static final int GET_COMMAND = 2;
    public static final int GET_COMMAND_RESULT = 3;
    public static final int REDIRECT_COMMAND = 4;
    public static final int CLUSTER_CHANGE_COMMAND = 5;
    public static final int CLUSTER_CHANGE_RESULT_COMMAND = 6;
    public static final int REQUEST_FAILED_COMMAND = 7;
    public static final int GET_CLUSTER_INFO_COMMAND = 8;
    public static final int GET_CLUSTER_INFO_RESULT_COMMAND = 9;

    public static final int APPEND_LOG_ENTRIES = 10;
    public static final int APPEND_LOG_ENTRIES_RESULT = 11;
    public static final int REQUEST_VOTE = 12;
    public static final int REQUEST_VOTE_RESULT = 13;
    public static final int PRE_VOTE = 14;
    public static final int PRE_VOTE_RESULT = 15;
    public static final int INSTALL_SNAPSHOT = 16;
    public static final int INSTALL_SNAPSHOT_RESULT = 17;
    public static final int SYNCING = 18;
}
