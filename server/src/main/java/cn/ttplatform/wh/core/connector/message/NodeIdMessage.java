package cn.ttplatform.wh.core.connector.message;

import cn.ttplatform.wh.constant.MessageType;
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
        return MessageType.NODE_ID;
    }

}
