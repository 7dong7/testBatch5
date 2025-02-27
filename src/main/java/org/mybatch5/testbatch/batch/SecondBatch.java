package org.mybatch5.testbatch.batch;

import lombok.RequiredArgsConstructor;
import org.mybatch5.testbatch.entity.WinEntity;
import org.mybatch5.testbatch.repository.WinRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.data.builder.RepositoryItemReaderBuilder;
import org.springframework.batch.item.data.builder.RepositoryItemWriterBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Sort;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Collections;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class SecondBatch {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;
    private final WinRepository winRepository;


// == job 등록 == //
    @Bean
    public Job secondJob() {
        return new JobBuilder("secondJob", jobRepository)
                .start(secondStep()) // 스템
//                .next() // 만약 추가적인 스텝이 있는 경우 체인 방식으로 추가 연결 가능
                .build();
    }

// == step 등록 == //
    @Bean
    public Step secondStep() {
        return new StepBuilder("secondStep", jobRepository)
                .<WinEntity, WinEntity>chunk(10, platformTransactionManager)
                .reader(winReader())
                .processor(trueProcessor())
                .writer(winWriter())
                .build();
    }

// == read, process, write == //
    @Bean // read
    public RepositoryItemReader<WinEntity> winReader() {
        return new RepositoryItemReaderBuilder<WinEntity>()
                .name("winReader")
                .pageSize(10) // 청크 단위 10개씩 처리
                .methodName("findByWinGreaterThanEqual") // winRepository 에서 "findByWinGreaterThanEqual" 의 메소드 호출
                .arguments(Collections.singletonList(10L)) // 여러 파라미터값을 한번에 보낼 수 있기 때문에 파라미터를 리스트 형태로 보낼 수 있다
                .repository(winRepository)
                .sorts(Map.of("id", Sort.Direction.ASC))
                .build();
    }

    @Bean // processor
    public ItemProcessor<WinEntity, WinEntity> trueProcessor() { // 람다식 형태
        return item -> {
            item.setReward(true);
            return item;
        };
    }

    @Bean
    public RepositoryItemWriter<WinEntity> winWriter() {
        return new RepositoryItemWriterBuilder<WinEntity>()
                .repository(winRepository)
                .methodName("save")
                .build();
    }
}
