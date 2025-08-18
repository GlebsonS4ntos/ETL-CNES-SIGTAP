package com.Glebson.ETL.Config;

import com.Glebson.ETL.Step.DownloadUnzipTasklet;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class BatchConfig {
    private final PlatformTransactionManager platformTransactionManager;
    private final JobRepository jobRepository;

    public BatchConfig(PlatformTransactionManager platformTransactionManager, JobRepository jobRepository) {
        this.platformTransactionManager = platformTransactionManager;
        this.jobRepository = jobRepository;
    }

    @Bean
    Job job(DownloadUnzipTasklet downloadUnzipTasklet){
        return new JobBuilder("job", jobRepository)
                .start(downloadStep(downloadUnzipTasklet))
                .build();
    }

    @Bean
    public Step downloadStep(DownloadUnzipTasklet downloadUnzipStep){
        return new StepBuilder("downloadStep", jobRepository)
                .tasklet(downloadUnzipStep, platformTransactionManager)
                .build();
    }
}