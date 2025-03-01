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

    public SixthBatch(JobRepository jobRepository,
                      PlatformTransactionManager platformTransactionManager,
                      @Qualifier("dataDBSource") DataSource dataSource) {
        this.jobRepository = jobRepository;
        this.platformTransactionManager = platformTransactionManager;
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


    /**
     *  === reader 정의 ===
     *  JdbcPagingItemReader<BeforeEntity>
     *      - <반환타입>
     *      - 스프링 배치에서 JDBC를 통해 데이터를 페이징 방식으로 읽어오는 reader 이다
     *
     *  dataSource(dataSource)
     *      - Bean 등록된 dataSource 를 설정한다 (data_db 사용)
     *      - MySQL 연결된 JDBC 객체
     *
     *  selectClause("SELECT id, username")
     *      - SQL 의 Select 절을 지정
     *
     *  fromClause("FROM BeforeEntity")
     *      - SQL 의 from 절을 설정
     *
     *  sortKeys(Map.of("id", Order.ASCENDING))
     *      - 정렬 기준을 설정한다
     *      - 페이징 방식으로 데이터를 조회하는 경우, 정렬 기준이 있어야 한다
     *      - 중복 조회 방지
     *
     *  rowMapper(new CustomBeforeRowMapper())
     *      - SQL 의 결과물 ResultSet 을 BeforeEntity 객체로 변환하는 역할을 한다
     *      - 
     *
     *
     */
    @Bean
    public JdbcPagingItemReader<BeforeEntity> sixthReader() {

        return new JdbcPagingItemReaderBuilder<BeforeEntity>() // reader 설정
                .name("sixthReader")        // reader 이름 설정
                .dataSource(dataSource)     // 사용 dataSource 설정 
                .selectClause("SELECT id, username")    // sql select 절 설정
                .fromClause("FROM BeforeEntity")        // sql from 절 설정
                .sortKeys(Map.of("id", Order.ASCENDING)) // 정렬 기준
                .rowMapper(new CustomBeforeRowMapper()) // SQL 결과물 ResultSet 객체를 BeforeEntity 객체로 변환
                .pageSize(10)       // 페이지 크기
                .build();           // reader 생성
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

    /**
     *  === writer 정의 ===
     *  JdbcBatchItemWriter<AfterEntity>
     *      - 여러 개의 데이터를 Batch 단위로 한꺼번에 insert 한다
     *      - 효율적으로 일괄 저장 (배치 처리) 한다
     *
     *  String sql = "INSERT INTO AfterEntity (username) VALUES (:username)";
     *  sql(sql)
     *      - sql 문이 작성된 String 을 설정
     *      - :username: Named Parameter 방식으로 값을 바인딩 한다
     *  
     *  itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
     *      - 객체의 필드값을 SQL 의 Named Parameter 와 매핑하는 역할을 한다
     *      - AfterEntity 객체의 필드 이름을 SQL :parameter 와 자동으로 매칭한다
     *      - 내부적으로 실행되는 메소드
     *          MapSqlParameterSource paramMap = new MapSqlParameterSource();
     *          ParamMap.addValue("username", afterEntity.getUsername());
     *  
     */
    @Bean
    public JdbcBatchItemWriter<AfterEntity> sixthWriter() {

        String sql = "INSERT INTO AfterEntity (username) VALUES (:username)";

        return new JdbcBatchItemWriterBuilder<AfterEntity>() // writer 설정
                .dataSource(dataSource)         // 접근 데이터소스
                .sql(sql)           // sql 설정
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>()) // 객체의 필드값을 sql 에 매핑해주는 객체 provider
                .build();   // writer 생성
    }

    /**
     *  itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
     *      - 객체의 필드값을 sql 에 자동으로 매핑해주는 provider
     *      - 객체 기반
     *
     *  itemSqlParameterSourceProvider(new MapItemSqlParameterSourceProvider<>())
     *      - map 의 값을 sql 에 자동으로 매핑해주는 provider
     *      - map 기반
     *      - 아래의 processor 에서 map 으로 넘겨주게 구현하고, writer 에서 BeanPropertyItemSqlParameterSourceProvider<>를 사용하면 된다
     *
     *  ItemSqlParameterSourceProvider 커스텀 구현 맨 아래
     *
     */
//    public class EntityToMapProcessor implements ItemProcessor<BeforeEntity, Map<String, Object>> {
//        @Override
//        public Map<String, Object> process(BeforeEntity item) {
//            Map<String, Object> paramMap = new HashMap<>();
//            paramMap.put("username", item.getUsername().toUpperCase()); // 변환 후 저장
//            return paramMap;
//        }
//    }

//    @Bean
//    public JdbcBatchItemWriter<AfterEntity> sixthWriter() {
//        String sql = "INSERT INTO AfterEntity (username) VALUES (:username)";
//
//        return new JdbcBatchItemWriterBuilder<AfterEntity>()
//                .dataSource(dataSource)
//                .sql(sql)
//                .itemSqlParameterSourceProvider(new MapItemSqlParameterSourceProvider<>())
//                .build();
//    }
    
    /**
     *  provider 를 커스텀으로 구현할 수 있다
     *  writer 에 사용
     */
    // 커스텀 provider
//    public class CustomSqlParameterSourceProvider implements ItemSqlParameterSourceProvider<AfterEntity> {
//        @Override
//        public SqlParameterSource createSqlParameterSource(AfterEntity item) {
//            MapSqlParameterSource paramSource = new MapSqlParameterSource();
//            paramSource.addValue("username", item.getUsername().toLowerCase()); // 데이터를 소문자로 변환 후 저장
//            return paramSource;
//        }
//    }

//    @Bean
//    public JdbcBatchItemWriter<AfterEntity> sixthWriter() {
//        String sql = "INSERT INTO AfterEntity (username) VALUES (:username)";
//
//        return new JdbcBatchItemWriterBuilder<AfterEntity>()
//                .dataSource(dataSource)
//                .sql(sql)
//                .itemSqlParameterSourceProvider(new CustomSqlParameterSourceProvider())
//                .build();
//    }

}
