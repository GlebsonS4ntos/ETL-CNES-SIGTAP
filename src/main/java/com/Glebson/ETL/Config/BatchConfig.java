package com.Glebson.ETL.Config;

import com.Glebson.ETL.Step.DownloadUnzipTasklet;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    private final PlatformTransactionManager platformTransactionManager;
    private final JobRepository jobRepository;
    private final FlowConfig flowConfig;

    public BatchConfig(PlatformTransactionManager platformTransactionManager,
                       JobRepository jobRepository,
                       FlowConfig flowConfig) {
        this.platformTransactionManager = platformTransactionManager;
        this.jobRepository = jobRepository;
        this.flowConfig = flowConfig;
    }

    @Bean
    public Job job(DownloadUnzipTasklet downloadUnzipTasklet) {
        Flow parallelFlow = flowConfig.processarArquivosParaleloFlow(jobRepository, platformTransactionManager);

        Step flowStep = new StepBuilder("processarArquivosParaleloFlowStep", jobRepository)
                .flow(parallelFlow)
                .build();

        return new JobBuilder("job", jobRepository)
                .start(downloadStep(downloadUnzipTasklet))
                .next(flowStep)
                .build();
    }

    @Bean
    public Step downloadStep(DownloadUnzipTasklet downloadUnzipTasklet) {
        return new StepBuilder("downloadStep", jobRepository)
                .tasklet(downloadUnzipTasklet, platformTransactionManager)
                .build();
    }
}