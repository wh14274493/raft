package cn.ttplatform.wh.cmd;

import cn.ttplatform.wh.common.Message;

/**
 * @author Wang Hao
 * @date 2021/2/19 19:08
 */
public interface Command extends Message {

    String getId();

    byte[] getCmd();
}