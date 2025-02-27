package org.mybatch5.testbatch.batch;

import lombok.RequiredArgsConstructor;
import org.apache.poi.ss.usermodel.Row;
import org.mybatch5.testbatch.entity.AfterEntity;
import org.mybatch5.testbatch.repository.AfterRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.data.builder.RepositoryItemWriterBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.IOException;

@Configuration
@RequiredArgsConstructor
public class fourthBatch {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;
    private final AfterRepository afterRepository;

    // ====== job 생성 ===== //
    @Bean
    public Job fourthJob() {
        System.out.println("fourth job");

        return new JobBuilder("fourthJob", jobRepository)
                .start(fourthStep())
                .build();
    }


    // ====== step ====== //
    @Bean
    public Step fourthStep() {

        return new StepBuilder("fourthStep", jobRepository)
                // import org.apache.poi.ss.usermodel.Row; 엑셇을 읽는 타입
                .<Row, AfterEntity>chunk(10, platformTransactionManager)
                .reader(excelReader())
                .processor(fourthProcessor())
                .writer(fourthAfterWriter())
                .build();
    }

    // == reader == //
    @Bean
    public ItemStreamReader<Row> excelReader() {

        try {
            return new ExcelRowReader("C:\\Users\\USER\\OneDrive\\문서\\통합 문서1.xlsx");
        } catch (IOException e) {
            return (ItemStreamReader<Row>) new RuntimeException(e);
        }
    }

    // == processor == //
    @Bean
    public ItemProcessor<Row, AfterEntity> fourthProcessor() {

        return new ItemProcessor<Row, AfterEntity>() {

            @Override
            public AfterEntity process(Row item) throws Exception {

                AfterEntity afterEntity = new AfterEntity();
                afterEntity.setUsername(item.getCell(0).getStringCellValue());

                return afterEntity;
            }
        };
    }

    // == write == //
    public RepositoryItemWriter<AfterEntity> fourthAfterWriter() {

        return new RepositoryItemWriterBuilder<AfterEntity>()
                .repository(afterRepository)
                .methodName("save")
                .build();
    }



}
