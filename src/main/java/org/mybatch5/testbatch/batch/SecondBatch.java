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

    /**
     *  === job 정의 ===
     */
    @Bean
    public Job secondJob() {
        return new JobBuilder("secondJob", jobRepository) // job 설정
                .start(secondStep())        // 정의된 스텝 등록
//                .next(다음 스텝)           // 추가적인 스템을 정의하면 이어서 실행시킬 수있다
                .build();           // job 생성
    }

    /**
     *  === step 정의 ===
     *  <WinEntity, WinEntity> chunk(10, platformTransactionManager)
     *      - <WinEntity, WinEntity>: <입력타입, 출력타입> 지정
     *      - chunk(10, platformTransactionManager): 청크방식(청크사이즈, 트랜잭션매니저)
     */
    @Bean
    public Step secondStep() {
        return new StepBuilder("secondStep", jobRepository) // step 설정
                .<WinEntity, WinEntity> chunk(10, platformTransactionManager) // 청크 방식 처리
                .reader(winReader())        // reader 등록
                .processor(trueProcessor()) // processor 등록
                .writer(winWriter())        // write 등록
                .build();   // step 생성
    }

    /**
     *  === reader 정의 ===
     *  methodName("findByWinGreaterThanEqual")
     *      - 레포지토리의 findByWinGreaterThanEqual 메소드를 호출한다
     *  
     *  arguments(Collections.singletonList(10L))
     *      - 위의에서 호출한 findByWinGreaterThanEqual 메소드의 파라미터 값으로 Long 타입 10 을 사용한다
     *      - 파라미터값으로 여러개의 값을 동시에 보낼 수 있기 때문에 Collections.singletonList(10L) 이렇게 보낸다
     */
    @Bean // read
    public RepositoryItemReader<WinEntity> winReader() {
        return new RepositoryItemReaderBuilder<WinEntity>() // reader 설정
                .name("winReader")          // reader 이름 정의
                .pageSize(10)               // 한 번에 10개씩 처리
                .methodName("findByWinGreaterThanEqual")    // winRepository 에서 "findByWinGreaterThanEqual" 의 메소드 호출
                .arguments(Collections.singletonList(10L))  // 여러 파라미터값을 한번에 보낼 수 있기 때문에 파라미터를 리스트 형태로 보낼 수 있다
                .repository(winRepository)                  // winRepository 저장소 사용
                .sorts(Map.of("id", Sort.Direction.ASC))    // 정렬 방식
                .build();   // reader 생성
    }


    /**
     *  === processor 정의 ===
     *  ItemProcessor<WinEntity, WinEntity>
     *      - <입력타입, 출력타입>
     *
     *  reader 에서 Win 의 값이 10 이상인 객체들을 조회했다
     *  그 다음, processor 단계에서 각 item<WinEntity> 들의 reward 를 true 로 바꾼느 처리를 한다
     *  람다식 처리
     */
    @Bean
    public ItemProcessor<WinEntity, WinEntity> trueProcessor() { // 람다식 형태
        return item -> {
            item.setReward(true);
            return item;
        };
    }

    /**
     *  === writer 정의 ===
     */
    @Bean
    public RepositoryItemWriter<WinEntity> winWriter() {
        return new RepositoryItemWriterBuilder<WinEntity>() // write 설정
                .repository(winRepository)              // 사용 레포지토리
                .methodName("save")                     // 레포지토리에 정의된 메소드명 save 호출
                .build();       // write 생성
    }
}
