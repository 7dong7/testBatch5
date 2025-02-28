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


/**
 *  엑셀 파일을 읽어서 DB 테이블에 저장하는 배치
 */
@Configuration
@RequiredArgsConstructor
public class FourthBatch {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;
    private final AfterRepository afterRepository;

    // === job 정의 ===
    @Bean
    public Job fourthJob() {
        System.out.println("fourth job");

        return new JobBuilder("fourthJob", jobRepository)
                .start(fourthStep())
                .build();
    }

    /**
     *  === step 정의 ===
     *  <Row, AfterEntity>chunk(10, platformTransactionManager)
     *      - <입력타입, 반환타입>
     *      - <Row>: 입력데이터로 Apache POI의 Row 객체 (엑셀의 한행)을 반환 타입 AfterEntity 객체로 변환한다
     */
    @Bean
    public Step fourthStep() {
        return new StepBuilder("fourthStep", jobRepository) // step 정의
                // import org.apache.poi.ss.usermodel.Row; 엑셇을 읽는 타입
                .<Row, AfterEntity> chunk(10, platformTransactionManager) // Row: 액셀의 한행  
                .reader(excelReader())
                .processor(fourthProcessor())
                .writer(fourthAfterWriter())
                .build();   // step 생성
    }

    /**
     *  === reader 정의 ===
     *  ItemStreamReader<Row> excelReader()
     *      - 배치1, 2 의 경우 RepositoryStreamReader 를 사용해 데이터베이스에서 데이터를 읽었다
     *      - 배치4 의 경우 엑셀 파일을 읽어서 데이터로 사용하기 때문에 ItemStreamReader 를 사용한다
     *
     *  new ExcelRowReader("C:\\Users\\USER\\OneDrive\\문서\\통합 문서1.xlsx");
     *      - 지정된 경로 "C:\\Users\\USER\\OneDrive\\문서\\통합 문서1.xlsx" 에 있는 엑셀 파일을 읽어 한 행씩 사용
     *      - ExcelRowReader() 개발자가 정의한 엑셀을 다루기위한 클래스
     */
    // == reader == //
    @Bean
    public ItemStreamReader<Row> excelReader() {

        try {
            return new ExcelRowReader("C:\\Users\\USER\\OneDrive\\문서\\통합 문서1.xlsx");
        } catch (IOException e) {
            return (ItemStreamReader<Row>) new RuntimeException(e);
        }
    }

    /**
     *  === processor 정의 ===
     *  ItemProcessor<Row, AfterEntity>
     *      - <입력타입, 반환타입>
     *      - 엑셀 파일의 한 행을 받아서, AfterEntity 객체로 반환
     *
     *  item.getCell(0).getStringCellValue()
     *      - item.getCell(0): Row 객체(엑셀의 한 행) 중 0번째 인덱스 열(cell) 을 가르킨다
     *      - getStringCellValue(): 반환된 cell 의 값을 문자열로 읽어온다 (텍스트 형식으로 추출)
     */
    @Bean
    public ItemProcessor<Row, AfterEntity> fourthProcessor() {

        return new ItemProcessor<Row, AfterEntity>() {
            @Override
            public AfterEntity process(Row item) throws Exception {

                AfterEntity afterEntity = new AfterEntity();
                afterEntity.setUsername(item.getCell(0).getStringCellValue()); // 각 행의 0번째 cell 읽어서 text 타입으로 추출

                return afterEntity;
            }
        };
    }

    // === writer 정의 ===
    public RepositoryItemWriter<AfterEntity> fourthAfterWriter() {

        return new RepositoryItemWriterBuilder<AfterEntity>() // writer 정의
                .repository(afterRepository)        // 사용 레포지토리
                .methodName("save")                 // 레포지토리에 정의된 메소도 호출
                .build();       // writer 생성
    }
}
