package org.mybatch5.testbatch.batch;

import org.mybatch5.testbatch.entity.AfterEntity;
import org.mybatch5.testbatch.entity.BeforeEntity;
import org.mybatch5.testbatch.repository.AfterRepository;
import org.mybatch5.testbatch.repository.BeforeRepository;
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

import java.util.Map;

@Configuration
public class FirstBatch {
    /**
     *  JobRepository
     *      - 배치 Job의 실행 상태, 메타데이터, 실행 기록 ( JonInstance, JobExecution, StepExecution )을 저장하고 관리한다
     *      - Job 과 Step 을 생성할 때 JpaRepository 를 전달하여 실행 정보를 기록하고, 재시작 (restart)등의 기능을 지원한다
     *
     *  PlatformTransactionManager
     *      - 배치 처리 중 데이터베이스 트랜잭션을 관리한다
     *      - 청크 기반 처리에서 각 청크(chunk) 단위의 데이터 읽기, 처리, 쓰기 작업을 하나의 트랜잭션으로 묶어 커믹하거나
     *          에러 발생 시 롤백하도록 한다
     */
    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;

    private final BeforeRepository beforeRepository;
    private final AfterRepository afterRepository;

    public FirstBatch(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager, BeforeRepository beforeRepository, AfterRepository afterRepository) {

        this.jobRepository = jobRepository;
        this.platformTransactionManager = platformTransactionManager;
        this.beforeRepository = beforeRepository;
        this.afterRepository = afterRepository;
    }

    /**
     *  ==== job 정의 ====
     *  new JobBuilder("firstJob", jobRepository)
     *      "firstJob" 이름의 Job 생성
     *
     *  start(firstStep())
     *      Job이 시작될 때 실행할 때 수행할 Step을 지정
     */
    @Bean
    public Job firstJob() {

        System.out.println("first job");

        return new JobBuilder("firstJob", jobRepository)
                .start(firstStep())
                .build(); // 최종
    }

    /**
     *  ==== Step 정의 ====
     *  new StepBuilder("firstStep", jobRepository)
     *      "firstStep" 이라는 이름의 Step 생성
     *
     *  <BeforeEntity, AfterEntity> chunk(10, platformTransactionManager)
     *      청크 기반 처리 방식을 사용
     *          - 데이터를 10건씩 읽어오고(processor로 변환 후) 한 묶음 단위로 트랜잭션을 커밋
     *      트랜잭션 매니저(PlatformTransactionManager)를 통해 각 청크 단위의 작업이 하나의 트랜잭션으로 처리한다
     *
     *      reader(beforeReader()): 데이터를 읽어오는 부분입니다.
     *      processor(middleProcessor()): 읽어온 데이터를 변환하는 부분입니다.
     *      writer(afterWriter()): 변환된 데이터를 저장하는 부분입니다.
     */
    @Bean
    public Step firstStep() {

        System.out.println("first step");

        // 청크 대량의 데이터를 페이징 처럼 부분부분 읽어오는 단위를 지정
        return new StepBuilder("firstStep", jobRepository)
                .<BeforeEntity, AfterEntity> chunk(10, platformTransactionManager)
                .reader(beforeReader()) // 읽는메소드자리
                .processor(middleProcessor()) // 처리메소드자리
                .writer(afterWriter()) // 쓰기메소드자리
                .build();
    }


    /**
     *  ==== reader ====
     *  new RepositoryItemReaderBuilder<BeforeEntity>()
     *
     *  name("beforeReader")
     *      reader 의 이름을 지정
     *
     *  pageSize(10)
     *      데이터를 10건씩 읽어온다
     *      청크의 크기과 같게해서 페이징 처리를 효율적으로
     *
     *  methodName("findAll")
     *      beforeRepository의 findAll 메서드를 호출하여 데이터를 읽어온다
     *
     *  repository(beforeRepository)
     *      실제 데이터를 읽어올 Repository를 지정
     *
     *  sorts(Map.of("id", Sort.Direction.ASC))
     *      읽어온 데이터를 Id 를 기준으로 오름차순으로 정렬한다
     */
    // 읽기
    @Bean // 해당 레포지 토리가 참조할 엔티티
    public RepositoryItemReader<BeforeEntity> beforeReader() {

        return new RepositoryItemReaderBuilder<BeforeEntity>()
                .name("beforeReader") // reader 에 대한 이름 
                .pageSize(10) // 10개씩 끊어서
                .methodName("findAll") // 데이터를 전부 읽는다
                .repository(beforeRepository) // 해당 레포지토리에서
                .sorts(Map.of("id", Sort.Direction.ASC)) // 정렬 방향
                .build();
    }


    /**
     *  ==== Processor ====
     *  읽어온 BeforeEntity 객체를 처리하여 AfterEntity로 변환
     *  실제 비즈니스 로직에서는 복잡한 데이터 변환, 검증, 계산 등의 작업을 할 수 있다
     *
     *
     *
     */
    @Bean // 처리
    public ItemProcessor<BeforeEntity, AfterEntity> middleProcessor() {

        return new ItemProcessor<BeforeEntity, AfterEntity>() {
            @Override // 리터로 부터 읽어온 값을 item으로 받음
            public AfterEntity process(BeforeEntity item) throws Exception {

                AfterEntity afterEntity = new AfterEntity();
                afterEntity.setUsername(item.getUsername());

                return afterEntity;
            }
        };
    }

    /**
     *  ==== Write ====
     *  AfterEntity 객체들을 데이터베이스에 저장하기 위해 Spring Data Repository를 사용하는 ItemWriter
     *
     *  repository(afterRepository)
     *      데이터를 저장할 Repository 를 지정
     *
     *  methodName("save")
     *      AfterRepository의 save 메서드를 사용하여 엔티티를 저장
     *      설정에 따라 processor에서 변환된 객체들이 10건씩 묶여 저장되고, 트랜잭션이 커밋된다
     */
    // 쓰기
    @Bean
    public RepositoryItemWriter<AfterEntity> afterWriter() {
        return new RepositoryItemWriterBuilder<AfterEntity>()
                .repository(afterRepository)
                .methodName("save")
                .build();
    }

}
