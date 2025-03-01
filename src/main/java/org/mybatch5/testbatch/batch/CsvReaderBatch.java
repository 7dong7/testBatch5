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
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class CsvReaderBatch {
    /**
     *  csv 파일을 읽어서 데이터 베이스에 저장하는 배치
     */
    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;
    private final DataSource dataSource;

    public CsvReaderBatch(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager, @Qualifier("dataDBSource") DataSource dataSource) {
        this.jobRepository = jobRepository;
        this.platformTransactionManager = platformTransactionManager;
        this.dataSource = dataSource;
    }
    
    // job 정의
    @Bean
    public Job csvReaderJob() {
        return new JobBuilder("csvReaderJob", jobRepository)
                .start(csvReaderStep())
                .build();
    }
    
    // step 정의
    @Bean
    public Step csvReaderStep() {
        return new StepBuilder("csvReaderStep", jobRepository)
                .<WinEntity, WinEntity>chunk(10, platformTransactionManager)
                .reader(csvReader())
                .processor(csvProcessor())
                .writer(csvWriter())
                .build();
    }
    
    // reader 정의
        // csv 파일 읽기
    @Bean
    public FlatFileItemReader<WinEntity> csvReader() {
        // csv 파일 읽기
        return new FlatFileItemReaderBuilder<WinEntity>()
                .name("csvReader")
//                .resource(new ClassPathResource("C:\\Users\\USER\\Desktop\\개발학습데이터\\csvreader.csv")) // 파일이 src/main/resources/csvreader.csv 아래에 있는 경우
                .resource(new FileSystemResource("C:\\Users\\USER\\Desktop\\개발학습데이터\\csvreader.csv")) // 파일이 로컬 파일 시스템인 경우
                .delimited()
                .names("id", "username", "win", "reward") // 헤더 컬럼명 지정
                .linesToSkip(1) // 첫 번째 행 스킵 (컴럼명 행)
                .fieldSetMapper(new BeanWrapperFieldSetMapper<WinEntity>() {{
                    setTargetType(WinEntity.class);
                }})
                .build();
    }

    // processor 정의
    @Bean
    public ItemProcessor<WinEntity, WinEntity> csvProcessor() {
        return item -> {
            WinEntity winEntity = new WinEntity();
            winEntity.setWin(item.getWin());
            winEntity.setUsername(item.getUsername());
            winEntity.setReward(item.getReward());

            return winEntity;
        };
    }
    
    // writer 정의 
        // DB 저장
    @Bean
    public JdbcBatchItemWriter<WinEntity> csvWriter() {

        String sql = "INSERT INTO WinEntity (username, win, reward) VALUES (:username, :win, :reward)";
        return new JdbcBatchItemWriterBuilder<WinEntity>()
                .dataSource(dataSource)
                .sql(sql)
//                .beanMapped() // BeanPropertyItemSqlParameterSourceProvider 사용
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .build();
    }
    /**
     *  == 객체와 sql 매핑 ==
     *  아래의 코드가 둘다 가능하다
     *
     *  beanMapped()
     *      - JdbcBatchItemWriterBuilder 에서 제공하는 편의 메소드
     *      - 내부적으로 BeanPropertyItemSqlParameterSourceProvider<>() 를 사용해서 객체를 SQL 파라미터에 매핑한다
     *          Item 객체의 getter 메소드를 통해 속성값을 가져와, SQL 쿼리의 이름 기반 파라미터(:username)에 매핑한다
     *          단, 기본적인 동작만 제공하고, 속성 이름과 쿼리파라미터 이름이 일치해야 한다
     *
     *  
     *  itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
     *      - 명시적으로 BeanPropertyItemSqlParameterSourceProvider<>() 을 사용 Item 객체의 속성을 반영하여 
     *        NameParameterJdbcTemplate 에서 사용할 파라미터 맵을 생성한다
     *      - 필요시 커스텀할 수 있다. (속성 이름 변환, 추가 로직 등)
     *
     *  결과적으로는 동일한 결과를 얻을 수 있지만, provider 을 명시적으로 사용하면 커스텀이 가능하다
     */

    // 커스텀 로직
//    @Bean
//    public JdbcBatchItemWriter<WinEntity> csvWriter() {
//        return new JdbcBatchItemWriterBuilder<WinEntity>()
//                .dataSource(dataSource)
//                .sql("INSERT INTO WinEntity (username, win, reward) VALUES (:user_name, :win_count, :is_rewarded)")
//                .itemSqlParameterSourceProvider(item -> {
//                    Map<String, Object> params = new HashMap<>();
//                    params.put("user_name", item.getUsername());
//                    params.put("win_count", item.getWin());
//                    params.put("is_rewarded", item.isReward());
//                    return new MapSqlParameterSource(params);
//                })
//                .build();
//    }
}
