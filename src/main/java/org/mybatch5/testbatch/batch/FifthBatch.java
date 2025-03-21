package org.mybatch5.testbatch.batch;

import lombok.RequiredArgsConstructor;
import org.mybatch5.testbatch.entity.BeforeEntity;
import org.mybatch5.testbatch.repository.BeforeRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.batch.item.data.builder.RepositoryItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Sort;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.IOException;
import java.util.Map;


/**
 *  DB 테이블을 읽어서 엑셀로 저장하는 배치
 */
@Configuration
@RequiredArgsConstructor
public class FifthBatch {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;
    private final BeforeRepository beforeRepository;

    // ===  job 정의 === //
    @Bean
    public Job fifthJob() {

        System.out.println("fifth job");

        return new JobBuilder("fifthJob", jobRepository)
                .start(fifthStep())
                .build();
    }
    
    // === step 정의 === //
    @Bean
    public Step fifthStep() {

        System.out.println("fifth step");

        return new StepBuilder("fifthStep", jobRepository)          // step 정의
                .<BeforeEntity, BeforeEntity> chunk(10, platformTransactionManager) // 청크 사이즈
                .reader(fifthBeforeReader())
                .processor(fifthProcessor())
                .writer(fifthwriter())
                .build();   // step 생성
    }
    
    // === reader 정의 === //
    @Bean
    public RepositoryItemReader<BeforeEntity> fifthBeforeReader() {

        RepositoryItemReader<BeforeEntity> reader = new RepositoryItemReaderBuilder<BeforeEntity>()
                .name("beforeReader")
                .pageSize(10)
                .methodName("findAll")
                .repository(beforeRepository)
                .sorts(Map.of("id", Sort.Direction.ASC))
                .build();

        // 전체 데이터 셋에서 어디까지 수행 했는지의 값을 저장하지 않음
        reader.setSaveState(false);

        return reader;
    }

    // === processor 정의 === //
    @Bean
    public ItemProcessor<BeforeEntity, BeforeEntity> fifthProcessor() {
        return item -> item;
    }


    // === writer 정의 === //
    @Bean
    public ItemStreamWriter<BeforeEntity> fifthwriter() {

        try {
            // 해당 경로의 엑셀 파일에 저장 혹은 생성
            return new ExcelRowWriter("C:\\Users\\USER\\OneDrive\\문서\\result.xlsx");
            //리눅스나 맥은 /User/형태로
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
