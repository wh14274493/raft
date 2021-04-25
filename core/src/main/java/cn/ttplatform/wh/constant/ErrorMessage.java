package cn.ttplatform.wh.constant;

/**
 * @author : wang hao
 * @date :  2020/8/15 22:02
 **/
public class ErrorMessage {

    private ErrorMessage() {
    }

    public static final String CLUSTER_SIZE_ERROR = "cluster size can't be less than 3";
    public static final String CREATE_FILE_ERROR = "create file error";
    public static final String RENAME_FILE_ERROR = "rename file error";
    public static final String CLOSE_FILE_ERROR = "close file error";
    public static final String SEEK_POSITION_ERROR = "seek file position error";
    public static final String WRITE_FILE_ERROR = "write file error";
    public static final String READ_FILE_ERROR = "read file error";
    public static final String READ_FILE_LENGTH_ERROR = "read file length error";
    public static final String TRUNCATE_FILE_ERROR = "truncate file error";
    public static final String LOAD_ENTRY_ERROR = "load entry error";
    public static final String READ_FAILED = "not enough content to read";
    public static final String CLUSTER_CHANGE_IN_PROGRESS = "cluster change processing is in progress.";
    public static final String CLUSTER_CHANGE_CANCELLED = "cluster change task had cancelled";
}
