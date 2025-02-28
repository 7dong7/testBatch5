package org.mybatch5.testbatch.batch;

import lombok.RequiredArgsConstructor;
import org.mybatch5.testbatch.entity.AfterEntity;
import org.mybatch5.testbatch.entity.BeforeEntity;
import org.mybatch5.testbatch.entity.CustomBeforeRowMapper;
import org.mybatch5.testbatch.repository.AfterRepository;
import org.mybatch5.testbatch.repository.BeforeRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.Map;

@Configuration
public class SixthBatch {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;

    private final DataSource dataSource; // 운영 데이터베이스

    private final AfterRepository afterRepository;
    private final BeforeRepository beforeRepository;

    public SixthBatch(JobRepository jobRepository,
                      PlatformTransactionManager platformTransactionManager,
                      BeforeRepository beforeRepository,
                      AfterRepository afterRepository,
                      @Qualifier("dataDBSource") DataSource dataSource) {
        this.jobRepository = jobRepository;
        this.platformTransactionManager = platformTransactionManager;
        this.beforeRepository = beforeRepository;
        this.afterRepository = afterRepository;
        this.dataSource = dataSource;
    }

    /**
     * JDBC 사용 성능 튜닝
     */
    
    // === job 정의 === //
    @Bean
    public Job sixthJob() {
        return new JobBuilder("sixthJob", jobRepository)
                .start(sixthStep())
                .build();
    }

    // === step 정의 === //
    @Bean
    public Step sixthStep() {

        return new StepBuilder("sixthStep", jobRepository)
                .<BeforeEntity, AfterEntity>chunk(10, platformTransactionManager)
                .reader(sixthReader())
                .processor(sixthProcessor())
                .writer(sixthWriter())
                .build();
    }

    // === reader 정의 === //
    @Bean
    public JdbcPagingItemReader<BeforeEntity> sixthReader() {

        return new JdbcPagingItemReaderBuilder<BeforeEntity>()
                .name("sixthReader")
                .dataSource(dataSource)
                .selectClause("SELECT id, username")
                .fromClause("FROM BeforeEntity")
                .sortKeys(Map.of("id", Order.ASCENDING))
                .rowMapper(new CustomBeforeRowMapper()) // 뭐지
                .pageSize(10)
                .build();
    }

    // === processor 정의 === //
    @Bean
    public ItemProcessor<BeforeEntity, AfterEntity> sixthProcessor() {

        return new ItemProcessor<BeforeEntity, AfterEntity>() {
            @Override
            public AfterEntity process(BeforeEntity item) throws Exception {

                AfterEntity afterEntity = new AfterEntity();
                afterEntity.setUsername(item.getUsername());

                return afterEntity;
            }
        };
    }

    // === writer 정의 === //
    @Bean
    public JdbcBatchItemWriter<AfterEntity> sixthWriter() {

        String sql = "INSERT INTO AfterEntity (username) VALUES (:username)";

        return new JdbcBatchItemWriterBuilder<AfterEntity>()
                .dataSource(dataSource)
                .sql(sql)
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .build();

    }
}
