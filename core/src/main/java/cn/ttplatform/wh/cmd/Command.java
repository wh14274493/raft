package cn.ttplatform.wh.cmd;

import cn.ttplatform.wh.support.Distributable;

/**
 * @author Wang Hao
 * @date 2021/2/19 19:08
 */
public interface Command extends Distributable {

    String getId();

}
