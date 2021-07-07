package cn.ttplatform.wh.exception;

/**
 * @author Wang Hao
 * @date 2021/5/18 20:28
 */
public class SnapshotParseException extends RuntimeException{

    public SnapshotParseException(String msg) {
        super(msg);
    }

    public SnapshotParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
