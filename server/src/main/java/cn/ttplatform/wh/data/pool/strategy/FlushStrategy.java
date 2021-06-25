package cn.ttplatform.wh.data.pool.strategy;

import cn.ttplatform.wh.data.pool.BlockCache.Block;

/**
 * @author Wang Hao
 * @date 2021/6/8 10:36
 */
public interface FlushStrategy {

    void flush(Block block);
    void flush();
}
