package cn.ttplatform.wh.constant;

import cn.ttplatform.wh.cmd.GetResponseCommand;
import cn.ttplatform.wh.cmd.SetResponseCommand;
import cn.ttplatform.wh.core.connector.message.AppendLogEntriesMessage;
import cn.ttplatform.wh.core.connector.message.AppendLogEntriesResultMessage;
import cn.ttplatform.wh.core.connector.message.InstallSnapshotMessage;
import cn.ttplatform.wh.core.connector.message.InstallSnapshotResultMessage;
import cn.ttplatform.wh.core.connector.message.NodeIdMessage;
import cn.ttplatform.wh.core.connector.message.PreVoteMessage;
import cn.ttplatform.wh.core.connector.message.PreVoteResultMessage;
import cn.ttplatform.wh.core.connector.message.RequestVoteMessage;
import cn.ttplatform.wh.core.connector.message.RequestVoteResultMessage;

/**
 * @author : wang hao
 * @date :  2020/8/16 0:29
 **/
public class MessageType {

    private MessageType() {
    }

    /**
     * {@link AppendLogEntriesMessage}
     */
    public static final int APPEND_LOG_ENTRIES = 0;

    /**
     * {@link AppendLogEntriesResultMessage}
     */
    public static final int APPEND_LOG_ENTRIES_RESULT = 1;

    /**
     * {@link RequestVoteMessage}
     */
    public static final int REQUEST_VOTE = 2;

    /**
     * {@link RequestVoteResultMessage}
     */
    public static final int REQUEST_VOTE_RESULT = 3;

    /**
     * {@link NodeIdMessage}
     */
    public static final int NODE_ID = 4;

    /**
     * {@link PreVoteMessage}
     */
    public static final int PRE_VOTE = 5;

    /**
     * {@link PreVoteResultMessage}
     */
    public static final int PRE_VOTE_RESULT = 6;

    /**
     * {@link InstallSnapshotMessage}
     */
    public static final int INSTALL_SNAPSHOT = 7;

    /**
     * {@link InstallSnapshotResultMessage}
     */
    public static final int INSTALL_SNAPSHOT_RESULT = 8;

    /**
     * {@link cn.ttplatform.wh.cmd.SetCommand}
     */
    public static final int SET_COMMAND = 9;

    /**
     * {@link SetResponseCommand}
     */
    public static final int SET_COMMAND_RESPONSE = 10;

    /**
     * {@link cn.ttplatform.wh.cmd.GetCommand}
     */
    public static final int GET_COMMAND = 11;

    /**
     * {@link GetResponseCommand}
     */
    public static final int GET_COMMAND_RESPONSE = 12;

    /**
     * {@link cn.ttplatform.wh.cmd.RedirectCommand}
     */
    public static final int REDIRECT_COMMAND = 13;

    /**
     * {@link cn.ttplatform.wh.cmd.RequestFailedCommand}
     */
    public static final int REQUEST_FAILED_COMMAND = 14;

}
