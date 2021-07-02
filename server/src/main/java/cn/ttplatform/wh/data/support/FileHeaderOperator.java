package cn.ttplatform.wh.data.support;

/**
 * @author Wang Hao
 * @date 2021/7/1 14:36
 */
public interface FileHeaderOperator {

    void recordFileSize(long size);

    long getFileSize();

    void force();
}
