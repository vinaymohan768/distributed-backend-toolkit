package com.vinay.toolkit;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableCaching
@EnableAsync
public class ToolkitApplication {
    public static void main(String[] args) {
        SpringApplication.run(ToolkitApplication.class, args);
    }
}
