package cn.ttplatform.lc.exception;

/**
 * @author : wang hao
 * @description : OperateFileException
 * @date :  2020/8/15 22:37
 **/
public class OperateFileException extends RuntimeException {

    public OperateFileException(String msg) {
        super(msg);
    }

    public OperateFileException(String message, Throwable cause) {
        super(message, cause);
    }
}
