package com.xhj.etcd.console;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * MiniEtcdConsoleApplication
 *
 * @author XJks
 * @description mini-etcd console 启动入口。
 */
@SpringBootApplication
@EnableScheduling
public class MiniEtcdConsoleApplication {

    public static void main(String[] args) {
        SpringApplication.run(MiniEtcdConsoleApplication.class, args);
    }
}
