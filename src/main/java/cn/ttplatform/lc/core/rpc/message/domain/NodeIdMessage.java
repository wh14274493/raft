package cn.ttplatform.lc.core.rpc.message.domain;

import cn.ttplatform.lc.constant.RpcMessageType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

/**
 * @author : wang hao
 * @date :  2020/8/16 12:39
 **/
@Getter
@Setter
@SuperBuilder
@AllArgsConstructor
public class NodeIdMessage extends AbstractMessage {

    @Override
    public int getType() {
        return RpcMessageType.NODE_ID;
    }

}
