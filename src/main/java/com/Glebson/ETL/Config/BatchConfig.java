package com.Glebson.ETL.Config;

import com.Glebson.ETL.Step.DeletePathTasklet;
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

    public BatchConfig(PlatformTransactionManager platformTransactionManager,
                       JobRepository jobRepository) {
        this.platformTransactionManager = platformTransactionManager;
        this.jobRepository = jobRepository;
    }

    @Bean
    public Job job(DownloadUnzipTasklet downloadUnzipTasklet, Flow processarArquivosParaleloFlow, Step deleteStep) {

        Step flowStep = new StepBuilder("processarArquivosParaleloFlowStep", jobRepository)
                .flow(processarArquivosParaleloFlow)
                .build();

        return new JobBuilder("job", jobRepository)
                .start(downloadStep(downloadUnzipTasklet))
                    .on("FAILED").to(deleteStep)
                .from(downloadStep(downloadUnzipTasklet))
                    .on("*").to(flowStep)
                .from(flowStep)
                    .on("*").to(deleteStep)
                .end()
                .build();
    }

    @Bean
    public Step downloadStep(DownloadUnzipTasklet downloadUnzipTasklet) {
        return new StepBuilder("downloadStep", jobRepository)
                .tasklet(downloadUnzipTasklet, platformTransactionManager)
                .build();
    }

    @Bean
    public Step deleteStep(DeletePathTasklet deletePathTasklet){
        return new StepBuilder("deleteStep", jobRepository)
                .tasklet(deletePathTasklet, platformTransactionManager)
                .build();
    }
}