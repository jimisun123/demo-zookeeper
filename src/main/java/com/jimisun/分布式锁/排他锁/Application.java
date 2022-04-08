package com.jimisun.分布式锁.排他锁;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

/**
 * 主入口
 * PS : 拍他锁主要是监听一个节点的删除事件，当真正释放锁当时候仍然会重复触发watcher事件，此处代码引入isDelete进行控制
 *
 * @author jimisun
 * @create 2022-04-08 3:36 PM
 **/
public class Application {

    static CuratorFramework client;
    static CountDownLatch countDownLatch;
    static Boolean isDelete = false;
    private static final String LOCK_NODE = "/x_node";

    public static void main(String[] args) throws Exception {
        connectCuratorClient();
        if (!getLock()) {
            startMonitoring(LOCK_NODE);
            countDownLatch = new CountDownLatch(1);
            countDownLatch.await();
        }
        System.out.println("我已经获得锁了...");
        System.out.println("开始处理业务中...");
        Thread.sleep(10000);
        isDelete = true;
        client.delete().forPath(LOCK_NODE);
        System.out.println("已经释放锁...");


    }

    public static Boolean getLock() {
        if (!isDelete) {
            //获取锁
            try {
                String nodeName = client.create().withMode(CreateMode.EPHEMERAL).forPath(LOCK_NODE, "".getBytes(StandardCharsets.UTF_8));
            } catch (KeeperException keeperException) {
                System.out.println("锁已被占用...");
                return false;
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
            return true;
        }
        return true;

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
                    if (getLock()) {
                        countDownLatch.countDown();
                    }
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

    /**
     * 创建会话
     */
    public static void connectCuratorClient() {
        RetryPolicy retry = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory.newClient("xxx", retry);
        client.start();
        System.out.println("zookeeper初始化连接成功：" + client);
    }


}
