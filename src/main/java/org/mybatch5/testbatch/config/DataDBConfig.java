package org.mybatch5.testbatch.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.HashMap;

/**
 * basePackages
 *  스캔할 JPA 레포지토리(DAO) 인터페이스가 위치한 패키지 지정
 *
 * entityManagerFactoryRef
 *  JPA 레포지토리들이 사용할 EntityManagerFactory의 Bean 이름을 지정한다
 *  dataEntityManage를 사용
 *
 * transactionManagerRef
 *  JPA 레포티로에서 사용할 트랜잭션 매니저의 Bean 이름을 지정
 */
@Configuration
@EnableJpaRepositories(
        basePackages = "org.mybatch5.testbatch.repository",
        entityManagerFactoryRef = "dataEntityManager", // 우리가 작성할
        transactionManagerRef = "dataTransactionManager" // 우리가 작성할
)
public class DataDBConfig {
    /**
     * 이와 같이 설정하면, 애플리케이션 내에서 메타 데이터와 비즈니스 데이터를 각각 다른 데이터베이스로 분리하여 관리할 수 있으며, 트랜잭션과 엔티티 관리 등도 별도로 제어할 수 있습니다.
     */
    
    // 비즈니스 DB 에 연결한 DataSource Bean 을 생성한다
    @Bean(name = "dataDBSource")
    @ConfigurationProperties(prefix = "spring.datasource-data")
    public DataSource dataDBSource() {
        return DataSourceBuilder.create().build();
    }

    // JPA EntityManager 를 생성하기 위한 팩토리 Bean을 설정한다
    @Bean
    public LocalContainerEntityManagerFactoryBean dataEntityManager() {
        LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();

        // 데이터 소스 지정
        em.setDataSource(dataDBSource());
        // 엔티티 클래스들이 위치한 패키지 설정
        em.setPackagesToScan(new String[]{"org.mybatch5.testbatch.entity"});
            // 만약 엔티티 클래스들을 관리하는 구조가 여러개인 경우 아래와 같이 작성할 수 있다
//        em.setPackagesToScan(new String[]{
//                "domain.member.entity",
//                "domain.post.entity",
//                "domain.comment.entity"
//        });

        // Hibernate 를 JPA 벤더 어댑터로 사용
        em. setJpaVendorAdapter(new HibernateJpaVendorAdapter());

        // 추가 JPA/Hibernate 속성 설정
        HashMap<String, Object> properties = new HashMap<>();
    // DB를 2개 설정하는 경우 하이번네이트 DB 세팅을 각각해주어야 되기 때문에 이렇게 설정해여 된다
        // 데이터베이스 스키마 자동 업데이트 (application.properties 의 database create, update, validate 같은거)
        properties.put("hibernate.hbm2ddl.auto", "update");
        // 실행시 SQL 로그를 콘솔에 출력
        properties.put("hibernate.show_sql", "true");
        em.setJpaPropertyMap(properties);

        return em;
    }
    
    // JPA를 사용하는 트랜잭션 매니저를 설정한다
    @Bean
    public PlatformTransactionManager dataTransactionManager() {
        JpaTransactionManager transactionManager = new JpaTransactionManager();

        transactionManager.setEntityManagerFactory(dataEntityManager().getObject());

        return transactionManager;
    }
}
