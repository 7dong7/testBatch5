package org.mybatch5.testbatch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class TestBatchApplication {

    public static void main(String[] args) {
        SpringApplication.run(TestBatchApplication.class, args);
    }

}
