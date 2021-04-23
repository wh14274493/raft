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

    public static final int SET_COMMAND_RESPONSE = 10;

    public static final int GET_COMMAND = 11;

    public static final int GET_COMMAND_RESPONSE = 12;

}
