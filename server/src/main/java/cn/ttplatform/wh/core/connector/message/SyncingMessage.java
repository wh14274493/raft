package cn.ttplatform.wh.core.connector.message;

import cn.ttplatform.wh.constant.DistributableType;

/**
 * @author Wang Hao
 * @date 2021/5/3 10:25
 */
public class SyncingMessage extends AbstractMessage {

    @Override
    public int getType() {
        return DistributableType.SYNCING;
    }
}
