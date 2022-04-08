package com.jimisun.负载均衡;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.net.InetAddress;
import java.util.List;
import java.util.Random;

/**
 * 主入口
 *
 * @author jimisun
 * @create 2022-04-08 12:53 PM
 **/
public class Application {

    static CuratorFramework client;

    static String SERVER_PATH = "/server";


    public static void main(String[] args) throws Exception {
        connectCuratorClient();
        // 注册地址信息到 Zookeeper
        registerAddressToZookeeper();
        randomRobin(); //轮询策略
        random();//随机策略

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


    /**
     * 注册地址信息到 Zookeeper
     * 服务启动时和服务手动上线时调用此方法
     *
     * @throws Exception Exception
     */
    public static void registerAddressToZookeeper() throws Exception {
        // 判断父节点是否存在，不存在则创建持久节点
        Stat stat = client.checkExists().forPath(SERVER_PATH);
        if (stat == null) {
            client.create().creatingParentsIfNeeded().forPath(SERVER_PATH);
        }
        // 获取本机地址
        String address = InetAddress.getLocalHost().getHostAddress();
        //上线
        if (client.checkExists().forPath(SERVER_PATH + "/" + address) == null) {
            client.create().withMode(CreateMode.EPHEMERAL).forPath(SERVER_PATH + "/" + address);
        }

    }


    /**
     * 轮询负载均衡策略
     *
     * @return
     */
    public static void randomRobin() throws Exception {
        List<String> serverList = client.getChildren().forPath(SERVER_PATH);
        for (int i = 0; i < serverList.size(); i++) {
            // 根据数组长度取模，顺序获取地址索引
            int i1 = i % serverList.size();
            // 根据索引获取服务器地址
            System.out.println(serverList.get(i1));
        }
    }


    /**
     * 轮询负载均衡策略
     *
     * @return
     */
    public static void random() throws Exception {
        List<String> serverList = client.getChildren().forPath(SERVER_PATH);
        for (int j = 0; j < serverList.size(); j++) {
            // 随机获取数组的索引
            int i = new Random().nextInt(serverList.size());
            // 根据索引获取服务器地址
            System.out.println(serverList.get(i));
        }
    }

}
