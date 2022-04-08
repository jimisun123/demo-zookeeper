package com.jimisun.分布式ID;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

/**
 * 主入口
 *
 * @author jimisun
 * @create 2022-04-08 12:36 PM
 **/
public class Main {

    static CuratorFramework client;

    /**
     * 创建会话
     */
    public static void connectCuratorClient() {
        RetryPolicy retry = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory.newClient("xxx", retry);
        client.start();
        System.out.println("zookeeper初始化连接成功：" + client);
    }

    public static void main(String[] args) throws Exception {
        //获取链接
        connectCuratorClient();
        //获取分布式id
        String id = client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath("/id-");
        System.out.print(id.replace("/","")
                .replace("-",""));
    }

}
