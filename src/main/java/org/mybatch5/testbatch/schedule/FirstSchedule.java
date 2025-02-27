package org.mybatch5.testbatch.schedule;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;

import java.text.SimpleDateFormat;
import java.util.Date;

//@Configuration
@RequiredArgsConstructor
public class FirstSchedule {

    private final JobLauncher jobLauncher;
    private final JobRegistry jobRegistry;

    /**
     *  cron = "10 * * * * *"
     *      10초에 한번씩 실행 (00:00:10, 00:01:10 등)
     *
     *  zone = "Asia/Seoul"
     *      서울 시간대 사용
     */
    @Scheduled(cron = "10 * * * * *", zone = "Asia/Seoul")
    public void runFirstJob() throws Exception {

        System.out.println("first schedule start");

        /**
         *  ==== 스케줄러 ====
         *  corn
         *
         */

        /**
         *  SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
         *      현재 날짜와 시간을 "yyyy-MM-dd HH:mm:ss"의 형식으로 문자열 변환한다
         *
         *  생성된 날짜 date 값을 job 의 파라미터로 만들어 전달하고 
         *  job을 실행시킨다
         */
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String date = dateFormat.format(new Date());

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("date", date)
                .toJobParameters();

        jobLauncher.run(jobRegistry.getJob("firstJob"), jobParameters);

    }
}
