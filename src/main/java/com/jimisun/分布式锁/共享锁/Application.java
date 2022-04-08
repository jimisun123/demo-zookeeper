package com.jimisun.分布式锁.共享锁;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 共享锁
 *
 * @author jimisun
 * @create 2022-04-08 1:59 PM
 **/
public class Application {

    static CuratorFramework client;
    static CountDownLatch countDownLatch;

    // 常量
    static class Constant {
        private static final int SESSION_TIMEOUT = 10000;
        private static final String LOCK_NODE = "/distributed_lock";
        private static final String CHILDREN_NODE = "/task_";
    }

    public static void main(String[] args) throws Exception {
        connectCuratorClient();

        //创建锁节点
        if (client.checkExists().forPath(Constant.LOCK_NODE) == null) {
            client.create().withMode(CreateMode.PERSISTENT).forPath(Constant.LOCK_NODE, "分布式锁节点".getBytes(StandardCharsets.UTF_8));
        }
        //创建一个锁
        String nodeName = client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(Constant.LOCK_NODE + Constant.CHILDREN_NODE, "".getBytes(StandardCharsets.UTF_8));
        System.out.println("已经创建锁" + nodeName);

        //尝试获取锁
        if (!acquireLock(nodeName)) {
            countDownLatch = new CountDownLatch(1);
            countDownLatch.await();
        }
        System.out.println("我已经获得锁了...");
        System.out.println("开始处理业务中...");
        Thread.sleep(10000);
        client.delete().forPath(nodeName);
        System.out.println("已经释放锁...");
    }


    /**
     * 创建会话
     */
    public static void connectCuratorClient() {
        RetryPolicy retry = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory.newClient("1.15.229.65:2181,1.15.229.65:2182,1.15.229.65:2183", retry);
        client.start();
        System.out.println("zookeeper初始化连接成功：" + client);
    }


    /**
     * 尝试获得锁 如果能获得锁 则返回true 否则对前一个节点进行删除监听
     *
     * @param lockName
     * @return
     * @throws InterruptedException
     */
    public static Boolean acquireLock(String lockName) throws Exception {
        //获取所有的子节点
        List<String> childNode = client.getChildren().forPath(Constant.LOCK_NODE);
        //针对自节点进行排序
        Collections.sort(childNode, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return Integer.parseInt(o1.split("_")[1]) - Integer.parseInt(o2.split("_")[1]);
            }
        });
        //判断当前节点是否在第一位
        int lockPostion = childNode.indexOf(lockName.split("/")[lockName.split("/").length - 1]);
        if (lockPostion < 0) {
            // 不存在该节点
            throw new RuntimeException("不存在的节点：" + lockName);
        } else if (lockPostion == 0) {
            return true;
        } else if (lockPostion > 0) {
            //未获取到锁 监听自节点前一个节点
            startMonitoring(Constant.LOCK_NODE + "/" + childNode.get(lockPostion - 1));
        }
        return false;
    }

    /**
     * 开启监听
     */
    public static void startMonitoring(String nodePath) {
        System.out.println("正在针对" + nodePath + "创建删除监听");
        // 构建 CuratorCache 实例
        CuratorCache cache = CuratorCache.build(client, nodePath);
        // 使用 Fluent 风格和 lambda 表达式来构建 CuratorCacheListener 的事件监听
        CuratorCacheListener listener = CuratorCacheListener.builder()
                //开启节点的删除监听
                .forDeletes(oldNode -> {
                    countDownLatch.countDown();
                })
                // 初始化
                .forInitialized(() -> System.out.println(">>> CuratorCacheListener 初始化"))
                // 构建
                .build();

        // 注册 CuratorCacheListener 到 CuratorCache
        cache.listenable().addListener(listener);
        // CuratorCache 开启缓存
        cache.start();
    }
}
