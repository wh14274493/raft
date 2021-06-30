package cn.ttplatform.wh.data.support;


/**
 * @author Wang Hao
 * @date 2021/6/8 10:36
 */
public interface FlushStrategy {

    void flush(AsyncFileOperator.Block block);

    void synFlushAll();
}
