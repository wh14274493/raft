package cn.ttplatform.lc;

/**
 * @author Wang Hao
 * @date 2020/10/28 下午8:47
 */
public class Demo {

    public static void main(String[] args) {
        char[] chars = "1234567".toCharArray();
        char[] chars1 = "ABCDEFG".toCharArray();

        Thread t1, t2;
        Object o = new Object();

        t1 = new Thread(() -> {
            synchronized (o) {
                for (char c : chars) {
                    System.out.println(c);
                    try {
                        o.notify();
                        o.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                o.notify();
            }
        });

        t2 = new Thread(() -> {
            synchronized (o) {
                for (char c : chars1) {
                    System.out.println(c);
                    try {
                        o.notify();
                        o.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                o.notify();
            }
        });
        t1.start();
        t2.start();
    }
}
