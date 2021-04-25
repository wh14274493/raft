package cn.ttplatform.wh.constant;

/**
 * @author : wang hao
 * @date :  2020/8/16 0:29
 **/
public class MessageType {

    private MessageType() {
    }

    public static final int APPEND_LOG_ENTRIES = 0;
    public static final int APPEND_LOG_ENTRIES_RESULT = 1;
    public static final int REQUEST_VOTE = 2;
    public static final int REQUEST_VOTE_RESULT = 3;
    public static final int NODE_ID = 4;
    public static final int PRE_VOTE = 5;
    public static final int PRE_VOTE_RESULT = 6;
    public static final int INSTALL_SNAPSHOT = 7;
    public static final int INSTALL_SNAPSHOT_RESULT = 8;
    public static final int SET_COMMAND = 9;
    public static final int SET_COMMAND_RESULT = 10;
    public static final int GET_COMMAND = 11;
    public static final int GET_COMMAND_RESULT = 12;
    public static final int REDIRECT_COMMAND = 13;
    public static final int CLUSTER_CHANGE_COMMAND = 14;
    public static final int CLUSTER_CHANGE_RESULT_COMMAND = 15;
    public static final int REQUEST_FAILED_COMMAND = 16;


}
