package org.mybatch5.testbatch.batch;

import lombok.RequiredArgsConstructor;
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
@RequiredArgsConstructor
public class FirstBatch {
    /**
     *  JobRepository
     *      - 배치 Job의 실행 상태, 메타데이터, 실행 기록 ( JonInstance, JobExecution, StepExecution )을 저장하고 관리한다
     *      - Job 과 Step 을 생성할 때 JpaRepository 를 전달하여 실행 정보를 기록하고, 재시작 (restart)등의 기능을 지원한다
     *
     *  PlatformTransactionManager
     *      - 배치 처리 중 데이터베이스 트랜잭션을 관리한다
     *      - 청크 기반 처리에서 각 청크(chunk) 단위의 데이터 읽기, 처리, 쓰기 작업을 하나의 트랜잭션으로 묶어 커밋하거나
     *        에러 발생 시 롤백하도록 한다
     */
    private final JobRepository jobRepository; // 저장소 역할
    private final PlatformTransactionManager platformTransactionManager; // 배치 트랜잭션 담당

    private final BeforeRepository beforeRepository;
    private final AfterRepository afterRepository;

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

        return new JobBuilder("firstJob", jobRepository) // job 설정 -> "이름", 저장소
                .start(firstStep()) // step 등록
                .build(); // job 생성
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
    public Step firstStep() { // step 생성
        
        System.out.println("first step");

        // 청크 대량의 데이터를 페이징 처럼 부분부분 읽어오는 단위를 지정
        return new StepBuilder("firstStep", jobRepository) // step 설정 -> "이름", 저장소
                .<BeforeEntity, AfterEntity> chunk(10, platformTransactionManager) // <입력타입, 출력타입> 청크 설정 -> 청크단위, 각 청크에대한 트랜잭션위임
                .reader(beforeReader())         // reader 데이터를 읽는 메소드 자리
                .processor(middleProcessor())   // processor 읽은 데이터를 처리하는 메소드 자리
                .writer(afterWriter())          // writer 처리된 데이터를 저장하는 메소드 자리
                .build(); // step 생성
    }

    /**
     *  ==== reader 정의 ====
     *  new RepositoryItemReaderBuilder<BeforeEntity>()
     *      - 저장소에서 아이템을 읽는다 (DB를 읽어온다)
     *
     *  name("beforeReader")
     *      reader 의 이름을 지정
     *
     *  pageSize(10)
     *      데이터를 10건씩 읽어온다
     *      청크의 크기과 같게해서 페이징 처리를 효율적으로 (청크단위와 크기를 맞춤)
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
    @Bean
    public RepositoryItemReader<BeforeEntity> beforeReader() { // <읽을 데이터 타입>

        return new RepositoryItemReaderBuilder<BeforeEntity>() // reader 설정
                .name("beforeReader")   // 해당 reader 이름
                .pageSize(10)           // 10개씩 끊어서 (청크 단위와 맞추는게 효율적)
                .methodName("findAll")  // 레포지토리의 메소드명
                .repository(beforeRepository) // 데이터를 읽을때 사용하는 레포지토리
                .sorts(Map.of("id", Sort.Direction.ASC)) // 정렬 방향 -> id 오름차순
                .build();   // reader 생성
    }

    /**
     *  ==== Processor 정의 ====
     *  ItemProcessor<BeforeEntity, AfterEntity>
     *      - <입력 데이터 타입, 출력 데이터 타입>
     *          
     *  ItemProcessor 인터페이스를 구현 - 익명 내부 클래스로 구현
     *      - process
     *
     *  읽어온 BeforeEntity 객체를 처리하여 AfterEntity 로 변환
     *  실제 비즈니스 로직에서는 복잡한 데이터 변환, 검증, 계산 등의 작업을 할 수 있다
     */
    @Bean // 처리
    public ItemProcessor<BeforeEntity, AfterEntity> middleProcessor() { // <입력 데이터 타입, 출력 데이터 타입>

        return new ItemProcessor<BeforeEntity, AfterEntity>() {
            @Override // reader 로 부터 읽어온 값을 item 으로 받음
            public AfterEntity process(BeforeEntity item) throws Exception { // 익명 내부 클래스로 ItemProcessor 인터페이스 구현

                AfterEntity afterEntity = new AfterEntity(); // 객체를 만들고
                afterEntity.setUsername(item.getUsername()); // 객체에 값을 담아서
                return afterEntity;                         // 리턴한다
            }
        };
    }

    /**
     *  ==== Write 정의 ====
     *  RepositoryItemWriter<AfterEntity>
     *      - 스프링 배치에서 처리된 <결과 타입> 을 저장한다
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
        return new RepositoryItemWriterBuilder<AfterEntity>() // writer 설정 -> <결과 타입>
                .repository(afterRepository)    // 저장할 저장소 설정
                .methodName("save")             // 저장소에서 사용할 메소드 설정
                .build();               // 객체 생성
    }
}