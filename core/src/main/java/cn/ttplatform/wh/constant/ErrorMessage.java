package cn.ttplatform.wh.constant;

/**
 * @author : wang hao
 * @date :  2020/8/15 22:02
 **/
public class ErrorMessage {

    private ErrorMessage() {
    }

    public static final String CLUSTER_SIZE_ERROR = "cluster size can't be less than 3";
    public static final String CLUSTER_CONFIG_ERROR = "cluster config must not be null.";
    public static final String CREATE_FILE_ERROR = "failed to create file.";
    public static final String RENAME_FILE_ERROR = "failed to rename file.";
    public static final String CLOSE_FILE_ERROR = "failed to close file.";
    public static final String WRITE_FILE_ERROR = "failed to write file.";
    public static final String READ_FILE_ERROR = "failed to read file.";
    public static final String READ_FILE_LENGTH_ERROR = "failed to read file size.";
    public static final String TRUNCATE_FILE_ERROR = "failed to truncate file.";
    public static final String READ_FAILED = "not enough content to read";
    public static final String CLUSTER_CHANGE_IN_PROGRESS = "cluster change processing is in progress.";
    public static final String CLUSTER_CHANGE_CANCELLED = "cluster change task had cancelled";
    public static final String NOT_SYNCING_PHASE = "current phase[%s] is not SYNCING.";
    public static final String NOT_STABLE_PHASE = "current phase[%s] is not STABLE.";
    public static final String NOT_NEW_OR_OLD_NEW_PHASE = "current phase[%s] is not NEW or OLD_NEW.";
    public static final String NOT_STABLE_OR_SYNCING_PHASE = "current phase[%S] is not STABLE or SYNCING.";
    public static final String NOT_OLD_NEW_PHASE = "current phase[%S] is not OLD_NEW.";
    public static final String ILLEGAL_MODE_STATE = "illegal mode state.";
    public static final String MESSAGE_TYPE_ERROR = "Types of messages that cannot be processed.";
    public static final String MESSAGE_PARSE_ERROR = "failed to parse message.";
    public static final String INDEX_OUT_OF_BOUND = "position[%d] out of bound[%d,%d].";
}
