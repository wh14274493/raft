package cn.ttplatform.wh.domain.cmd;

import cn.ttplatform.wh.domain.message.Message;

/**
 * @author Wang Hao
 * @date 2021/2/19 19:08
 */
public interface Command extends Message {

    String getId();

    byte[] getCmd();
}
