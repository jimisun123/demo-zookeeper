package com.jimisun.一致性配置管理;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.nio.charset.StandardCharsets;
import java.util.Random;

/**
 * 主入口
 *
 * @author jimisun
 * @create 2022-04-08 1:20 PM
 **/
public class Application {

    static CuratorFramework client;
    static String nodePath = "/commConfig";


    public static void main(String[] args) throws Exception {
        connectCuratorClient();
        startMonitoring(client);
        while (true) {
            createOrUpdateCommConfig();
            Thread.sleep(3000);
        }
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
     * 创建或更新zookeeper配置信息
     */
    public static void createOrUpdateCommConfig() throws Exception {
        if (client.checkExists().forPath(nodePath) != null) {
            client.setData().forPath(nodePath, String.valueOf(new Random().nextInt(200)).getBytes(StandardCharsets.UTF_8));
        } else {
            client.create().withMode(CreateMode.PERSISTENT).forPath(nodePath, "0000".getBytes(StandardCharsets.UTF_8));
        }

    }


    /**
     * 开启监听
     *
     * @param client CuratorFramework
     */
    public static void startMonitoring(CuratorFramework client) {
        // 构建 CuratorCache 实例
        CuratorCache cache = CuratorCache.build(client, nodePath);
        // 使用 Fluent 风格和 lambda 表达式来构建 CuratorCacheListener 的事件监听
        CuratorCacheListener listener = CuratorCacheListener.builder()
                // 开启对节点更新事件的监听
                .forChanges((oldNode, newNode) -> {
                    System.out.println("有客户端更改了commConfig" + oldNode + ">>>>>" + newNode);
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
