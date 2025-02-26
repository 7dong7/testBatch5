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

@Configuration
@EnableJpaRepositories(
        basePackages = "org.mybatch5.testbatch.repository",
        entityManagerFactoryRef = "dataEntityManager", // 우리가 작성할
        transactionManagerRef = "dataTransactionManager" // 우리가 작성할
)
public class DataDBConfig {

    @Bean
    @ConfigurationProperties(prefix = "spring.datasource-data")
    public DataSource dataDBSource() {
        return DataSourceBuilder.create().build();
    }

    // 엔티티들이 모이게될 DB 설정
    @Bean
    public LocalContainerEntityManagerFactoryBean dataEntityManager() {
        LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();

        em.setDataSource(dataDBSource());
        em.setPackagesToScan(new String[]{"org.mybatch5.testbatch.entity"});
        em. setJpaVendorAdapter(new HibernateJpaVendorAdapter());

        HashMap<String, Object> properties = new HashMap<>();
        // DB를 2개 설정하는 경우 하이번네이트 DB 세팅을 각각해주어야 되기 때문에 이렇게 설정해여 된다
        properties.put("hibernate.hbm2ddl.auto", "update");
        properties.put("hibernate.show_sql", "true");
        em.setJpaPropertyMap(properties);

        return em;
    }

    @Bean
    public PlatformTransactionManager dataTransactionManager() {
        JpaTransactionManager transactionManager = new JpaTransactionManager();

        transactionManager.setEntityManagerFactory(dataEntityManager().getObject());

        return transactionManager;
    }
}
