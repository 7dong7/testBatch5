package org.mybatch5.testbatch.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@ResponseBody
@RequiredArgsConstructor
public class MainController {

    private final JobLauncher jobLauncher; // job을 실행하는 컴포넌트

    // 애플리케이션에 내에 등록된 배치 job들을 관리하는 레지스트리로 job의 이름을 기반으로 해당 job을 조회할 수 있다
    private final JobRegistry jobRegistry;

    // first 배치 - db의 테이블을 db의 다른 테이블로 복제하느 배치
        // beforeEntity 테이블을 AfterEntity 테이블로 값을 복사한다
    @GetMapping("/first")
    public String firstApi(@RequestParam("value") String value) throws Exception {
        /**
         *  new JobParametersBuilder()
         *      job에 전달할 파라미터를 생성
         *
         *  addString("date", value)
         *      date 라는 이름의 파라미터에, 요청으로 전달받은 value 값을 저장
         *
         *  toJobParameters()
         *      생성된 jobParameters 는 배치 job의 실행에 필요한 입력값으로 사용된다
         */
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("data", value)
                .toJobParameters();

        /**
         *  jobRegistry.getJob("firstJob")
         *      "firstJob" 이라는 이름의 job 조회
         *
         *  jobLauncher.run()
         *      조회된 job을 jobLauncher 를 통해 실행된다
         *      생성한 jobParameters 를 함께 전달되어, 배치 작업 내에서 해당 파라미터를 사용할 수 있다
         */
        jobLauncher.run(jobRegistry.getJob("firstJob"), jobParameters);

        return "ok";
    }
    
    // second 배치 - 테이블의 특정 컬럼의 값을 확인해 다른 컬럼의 값을 변경하는 배치
        // win 컬럼이 10을 넘으면 reward 컬럼에 true 추가
    @GetMapping("/second")
    public String secondApi(@RequestParam("value") String value) throws Exception {

        // 잡 파라미터 생성
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("data", value)
                .toJobParameters();

        jobLauncher.run(jobRegistry.getJob("secondJob"), jobParameters);

        return "ok";
    }

    // fourth 배치 - 엑셀을 db로 읽어오는 배치
    @GetMapping("/fourth")
    public String fourthApi(@RequestParam("value") String value) throws Exception {
        
        // jop 파라미터 생성
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("data", value)
                .toJobParameters();

        // jop 실행
        jobLauncher.run(jobRegistry.getJob("fourthJob"), jobParameters);

        return "ok";
    }

    // fourth 배치 - 엑셀을 db로 읽어오는 배치
    @GetMapping("/fifth")
    public String fifthApi(@RequestParam("value") String value) throws Exception {

        // jop 파라미터 생성
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("data", value)
                .toJobParameters();

        // jop 실행
        jobLauncher.run(jobRegistry.getJob("fifthJob"), jobParameters);

        return "ok";
    }

    // sixth 배치 - 
    @GetMapping("/sixth")
    public String sixthApi(@RequestParam("value") String value) throws Exception {

        // jop 파라미터 생성
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("data", value)
                .toJobParameters();

        // jop 실행
        jobLauncher.run(jobRegistry.getJob("sixthJob"), jobParameters);

        return "ok";
    }

    // seventh 배치 - WinEntity win>=10 인 경우 WinEntity reward = true 변경 배치
    @GetMapping("/seventh")
    public String seventhApi(@RequestParam("value") String value) throws Exception {

        // jop 파라미터 생성
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("data", value)
                .toJobParameters();

        // jop 실행
        jobLauncher.run(jobRegistry.getJob("seventhJob"), jobParameters);

        return "ok";
    }

    // csvReaderJob 배치
    @GetMapping("/csvReaderJob")
    public String csvReaderJobApi(@RequestParam("value") String value) throws Exception {

        // jop 파라미터 생성
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("data", value)
                .toJobParameters();

        // jop 실행
        jobLauncher.run(jobRegistry.getJob("csvReaderJob"), jobParameters);

        return "ok";
    }

    // ReadExcelJDBCWriterBatch 배치
    @GetMapping("/excelReadJob")
    public String excelReadJobApi(@RequestParam("value") String value) throws Exception {

        // jop 파라미터 생성
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("data", value)
                .toJobParameters();

        // jop 실행
        jobLauncher.run(jobRegistry.getJob("excelReadJob"), jobParameters);

        return "ok";
    }

}
