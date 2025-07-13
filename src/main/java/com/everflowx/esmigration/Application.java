package com.everflowx.esmigration;

import cn.hutool.core.util.StrUtil;
import cn.hutool.extra.spring.SpringUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 启动类
 *
 * @author everflowx
 */
@Slf4j
@EnableScheduling
@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(new Class[]{Application.class, SpringUtil.class}, args);
        startUp();
    }

    private static void startUp() {
        System.out.println("=====> os.name:" + System.getProperty("os.name"));
        String url = "http://localhost:6618/doc.html";
        String property = System.getProperty("os.name").toLowerCase();
        try {
            if (StrUtil.isNotBlank(property) && property.contains("windows")) {
                // windows启动方式
                Runtime.getRuntime().exec("cmd /c start " + url);
            } else {
                // Linux
                Runtime.getRuntime().exec("xdg-open " + url);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
