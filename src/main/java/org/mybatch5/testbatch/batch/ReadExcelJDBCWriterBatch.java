package org.mybatch5.testbatch.batch;

import org.mybatch5.testbatch.entity.WinEntity;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class ReadExcelJDBCWriterBatch {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;
    private final DataSource dataSource;

    public ReadExcelJDBCWriterBatch(JobRepository jobRepository,
                                    PlatformTransactionManager platformTransactionManager,
                                    @Qualifier("dataDBSource") DataSource dataSource) {
        this.jobRepository = jobRepository;
        this.platformTransactionManager = platformTransactionManager;
        this.dataSource = dataSource;
    }

    // job 정의
    @Bean
    public Job excelReadJob() {
        return new JobBuilder("excelReadJob", jobRepository)
                .start(excelReadStep())
                .build();
    }

    // step 정의
    @Bean
    public Step excelReadStep() {
        return new StepBuilder("excelReadStep", jobRepository)
                .<WinEntity, WinEntity>chunk(10, platformTransactionManager)
                .reader(excelReader())
                .processor(excelProcessor())
                .writer(excelWriter())
                .build();
    }

    // reader 정의
    // csv 파일 읽기
    @Bean
    public ItemStreamReader<WinEntity> excelReader() {
        try {
            return new ExcelReader("C:\\Users\\USER\\OneDrive\\excelReaderJDBCwrite.xlsx");
        } catch (Exception e) {
            throw new ItemStreamException("Failed to open EXCEL FILE", e);
        }

    }

    // processor 정의
    @Bean
    public ItemProcessor<WinEntity, WinEntity> excelProcessor() {
        return item -> item;
    }

    // writer 정의
    // DB 저장
    @Bean
    public JdbcBatchItemWriter<WinEntity> excelWriter() {

        String sql = "INSERT INTO WinEntity (username, win, reward) VALUES (:username, :win, :reward)";
        return new JdbcBatchItemWriterBuilder<WinEntity>()
                .dataSource(dataSource)
                .sql(sql)
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .build();
    }
}
