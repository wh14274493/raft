package cn.ttplatform.wh.constant;

import cn.ttplatform.wh.domain.cmd.GetResponseCommand;
import cn.ttplatform.wh.domain.cmd.SetResponseCommand;
import cn.ttplatform.wh.domain.message.AppendLogEntriesMessage;
import cn.ttplatform.wh.domain.message.AppendLogEntriesResultMessage;
import cn.ttplatform.wh.domain.message.InstallSnapshotMessage;
import cn.ttplatform.wh.domain.message.InstallSnapshotResultMessage;
import cn.ttplatform.wh.domain.message.NodeIdMessage;
import cn.ttplatform.wh.domain.message.PreVoteMessage;
import cn.ttplatform.wh.domain.message.PreVoteResultMessage;
import cn.ttplatform.wh.domain.message.RequestVoteMessage;
import cn.ttplatform.wh.domain.message.RequestVoteResultMessage;

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
     * {@link cn.ttplatform.wh.domain.cmd.SetCommand}
     */
    public static final int SET_COMMAND = 9;

    /**
     * {@link SetResponseCommand}
     */
    public static final int SET_COMMAND_RESPONSE = 10;

    /**
     * {@link cn.ttplatform.wh.domain.cmd.GetCommand}
     */
    public static final int GET_COMMAND = 11;

    /**
     * {@link GetResponseCommand}
     */
    public static final int GET_COMMAND_RESPONSE = 12;

}
