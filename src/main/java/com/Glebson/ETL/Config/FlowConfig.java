package com.Glebson.ETL.Config;

import com.Glebson.ETL.Domain.Sigtap.Procedimento;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ConcurrentTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import org.springframework.batch.core.job.flow.Flow;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Configuration
public class FlowConfig {

    @Autowired
    private FlatFileItemReader<Procedimento> procedimentoReader;

    @Autowired
    private JdbcBatchItemWriter<Procedimento> procedimentoItemWriter;

    @Bean
    public TaskExecutor virtualTaskExecutor(){
        return new ConcurrentTaskExecutor(Executors.newVirtualThreadPerTaskExecutor());
    }

    @Bean
    public Step procedimentoStep(JobRepository repository, PlatformTransactionManager transactionManager){
        return new StepBuilder("procedimentoStep", repository)
                .<Procedimento, Procedimento>chunk(1000, transactionManager)
                .reader(procedimentoReader)
                .writer(procedimentoItemWriter)
                .taskExecutor(virtualTaskExecutor())
                .build();
    }

    @Bean
    public Flow procedimentoFlow(JobRepository repository, PlatformTransactionManager transactionManager){
        return new FlowBuilder<Flow>("procedimentoFlow")
                .start(procedimentoStep(repository, transactionManager))
                .build();
    }

    @Bean
    public Flow processarArquivosParaleloFlow(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        ThreadPoolTaskExecutor taskExecutor =  new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(2);
        taskExecutor.initialize();

        return new FlowBuilder<Flow>("processarArquivosParaleloFlow")
                .split(taskExecutor)
                .add(
                    procedimentoFlow(jobRepository, transactionManager)
                ).build();
    }
}

