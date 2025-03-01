package org.mybatch5.testbatch.batch;

import org.mybatch5.testbatch.entity.WinEntity;
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
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.Map;

@Configuration
public class SeventhBatch {

    private final JobRepository jobRepository;

    private final PlatformTransactionManager platformTransactionManager;

    private final DataSource dataSource;

    public SeventhBatch(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager, @Qualifier("dataDBSource") DataSource dataSource) {
        this.jobRepository = jobRepository;
        this.platformTransactionManager = platformTransactionManager;
        this.dataSource = dataSource;
    }

    // === job 정의 === //
    @Bean
    public Job seventhJob() {
        return new JobBuilder("seventhJob", jobRepository)
                .start(seventhStep())
                .build();
    }

    // === step 정의 === //
    @Bean
    public Step seventhStep() {

        return new StepBuilder("seventhStep", jobRepository)
                .<WinEntity, WinEntity>chunk(100, platformTransactionManager)
                .reader(seventhReader())
                .processor(seventhProcessor())
                .writer(seventhWriter())
                .build();
    }


    // === reader 정의 === //
    @Bean
    public JdbcPagingItemReader<WinEntity> seventhReader() {

        return new JdbcPagingItemReaderBuilder<WinEntity>()
                .name("seventhReader")
                .dataSource(dataSource)
                .selectClause("SELECT id, win, reward")
                .fromClause("FROM WinEntity")
                .whereClause("WHERE win >= 10")
                .sortKeys(Map.of("id", Order.ASCENDING))
                .rowMapper(new BeanPropertyRowMapper<>(WinEntity.class)) // 자동 매핑 // 복잡한 구조가 필요한경우 커스텀해서 사용하느게 좋음
                .pageSize(10)
                .build();
    }

    // === processor 정의 === //
    @Bean
    public ItemProcessor<WinEntity, WinEntity> seventhProcessor() {
        return WinEntity -> {
            WinEntity.setReward(true);
            return WinEntity;
        };
    }

    // === writer 정의 === //
    @Bean
    public JdbcBatchItemWriter<WinEntity> seventhWriter() {

        String sql = "UPDATE WinEntity SET reward = :reward WHERE id = :id";

        return new JdbcBatchItemWriterBuilder<WinEntity>()
                .dataSource(dataSource)
                .sql(sql)
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .build();
    }

}
