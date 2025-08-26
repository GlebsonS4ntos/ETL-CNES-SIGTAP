package com.Glebson.ETL.Config;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ConcurrentTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import org.springframework.batch.core.job.flow.Flow;

import java.util.concurrent.Executors;

@Configuration
public class FlowConfig {

    @Bean
    public TaskExecutor virtualTaskExecutor(){
        return new ConcurrentTaskExecutor(Executors.newVirtualThreadPerTaskExecutor());
    }

    @Bean
    public Flow processarArquivosParaleloFlow(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                              Step procedimentoStep,
                                              Step tipoProcedimentoStep,
                                              Step compatibilidadeProcedimentosSecundarioStep,
                                              Step compatibilidadeProcedimentosCBOStep,
                                              Step estadoStep,
                                              Step municipioStep) {
        ThreadPoolTaskExecutor taskExecutor =  new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(2);
        taskExecutor.initialize();

        return new FlowBuilder<Flow>("processarArquivosParaleloFlow")
                .split(taskExecutor)
                .add(
                    procedimentoFlow(jobRepository, transactionManager, procedimentoStep),
                    tipoProcedimentoFlow(jobRepository, transactionManager, tipoProcedimentoStep),
                    compatibilidadeProcedimentosSecundarioFlow(jobRepository, transactionManager, compatibilidadeProcedimentosSecundarioStep),
                    compatibilidadeProcedimentosCBOFlow(jobRepository, transactionManager, compatibilidadeProcedimentosCBOStep),
                    estadoFlow(jobRepository, transactionManager, estadoStep),
                    municipioFlow(jobRepository, transactionManager, municipioStep)

                ).build();
    }

    @Bean
    public Flow procedimentoFlow(JobRepository repository, PlatformTransactionManager transactionManager,
                                 Step procedimentoStep){
        return new FlowBuilder<Flow>("procedimentoFlow")
                .start(procedimentoStep)
                .build();
    }

    @Bean
    public Flow tipoProcedimentoFlow(JobRepository repository, PlatformTransactionManager transactionManager,
                                     Step tipoProcedimentoStep){
        return new FlowBuilder<Flow>("tipoProcedimentoFlow")
                .start(tipoProcedimentoStep)
                .build();
    }

    @Bean
    public Flow compatibilidadeProcedimentosSecundarioFlow(JobRepository repository, PlatformTransactionManager transactionManager,
                                                           Step compatibilidadeProcedimentosSecundarioStep){
        return new FlowBuilder<Flow>("compatibilidadeProcedimentosSecundarioFlow")
                .start(compatibilidadeProcedimentosSecundarioStep)
                .build();
    }

    @Bean
    public Flow compatibilidadeProcedimentosCBOFlow(JobRepository repository, PlatformTransactionManager transactionManager,
                                                    Step compatibilidadeProcedimentosCBOStep){
        return new FlowBuilder<Flow>("compatibilidadeProcedimentosCBOFlow")
                .start(compatibilidadeProcedimentosCBOStep)
                .build();
    }

    @Bean
    public Flow estadoFlow(JobRepository repository, PlatformTransactionManager transactionManager,
                                                    Step estadoStep){
        return new FlowBuilder<Flow>("estadoFlow")
                .start(estadoStep)
                .build();
    }

    @Bean
    public Flow municipioFlow(JobRepository repository, PlatformTransactionManager transactionManager,
                                                    Step municipioStep){
        return new FlowBuilder<Flow>("municipioFlow")
                .start(municipioStep)
                .build();
    }
}

