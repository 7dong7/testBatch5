package org.mybatch5.testbatch.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class MetaDBConfig {
    /**
     * @Primary
     *  여러 개의 동일한 타입 Bean이 존재할 때 우선적으로 주입되어야할 Bean임을 지정한다
     *  현재 meta_db 관련 Bean들을 기본값으로 설정
     *
     * @ConfigurationProperties(prefix = "spring.datasource-meta")
     *  애플리케이션 application.properties 에서 spring.datasource-meta 로 시작하는 설정 값을 읽어 DataSource를 구성한다
     *  
     * metaTransactionManager() 메소드
     *  DataSourceTransactionManager를 사용하여 metaDBSource()로 생성한 DataSource에 대한 트랜잭션 관리자를 생성
     *  meta_db 에 대한 트랜잭션 처리가 안정적으로 이루어진다
     */
    @Primary
    @Bean
    @ConfigurationProperties(prefix = "spring.datasource-meta")
    public DataSource metaDBSource() {
        return DataSourceBuilder.create().build();
    }

    @Primary
    @Bean
    public PlatformTransactionManager metaTransactionManager() {
        return new DataSourceTransactionManager(metaDBSource());
    }
}
