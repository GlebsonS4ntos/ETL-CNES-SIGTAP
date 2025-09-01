package com.Glebson.ETL.Config;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Qualifier;
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
                                              @Qualifier("procedimentoStep") Step procedimentoStep,
                                              @Qualifier("tipoProcedimentoStep") Step tipoProcedimentoStep,
                                              @Qualifier("compatibilidadeProcedimentosSecundarioStep") Step compatibilidadeProcedimentosSecundarioStep,
                                              @Qualifier("compatibilidadeProcedimentosCBOStep") Step compatibilidadeProcedimentosCBOStep,
                                              @Qualifier("estadoStep") Step estadoStep,
                                              @Qualifier("municipioStep") Step municipioStep,
                                              @Qualifier("cboStep") Step cboStep,
                                              @Qualifier("profissionalStep") Step profissionalStep,
                                              @Qualifier("estabelecimentoStep") Step estabelecimentoStep,
                                              @Qualifier("cargaHorariaCboProfissionalStep") Step cargaHorariaCboProfissionalStep,
                                              @Qualifier("equipeStep") Step equipeStep,
                                              @Qualifier("estabelecimentoEquipeProfissionalStep") Step estabelecimentoEquipeProfissionalStep) {
        ThreadPoolTaskExecutor taskExecutor =  new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(2);
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
                    municipioFlow(jobRepository, transactionManager, municipioStep),
                    cboFlow(jobRepository, transactionManager, cboStep),
                    profissionalFlow(jobRepository, transactionManager, profissionalStep),
                    estabelecimentoFlow(jobRepository, transactionManager, estabelecimentoStep),
                    cargaHorariaCboProfissionalFlow(jobRepository, transactionManager, cargaHorariaCboProfissionalStep),
                    equipeFlow(jobRepository, transactionManager, equipeStep),
                    estabelecimentoEquipeProfissionalFlow(jobRepository, transactionManager, estabelecimentoEquipeProfissionalStep)
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

    @Bean
    public Flow cboFlow(JobRepository repository, PlatformTransactionManager transactionManager,
                              Step cboStep){
        return new FlowBuilder<Flow>("cboFlow")
                .start(cboStep)
                .build();
    }

    @Bean
    public Flow profissionalFlow(JobRepository repository, PlatformTransactionManager transactionManager,
                                    Step profissionalStep){
        return new FlowBuilder<Flow>("profissionalFlow")
                .start(profissionalStep)
                .build();
    }

    @Bean
    public Flow estabelecimentoFlow(JobRepository repository, PlatformTransactionManager transactionManager,
                                 Step estabelecimentoStep){
        return new FlowBuilder<Flow>("estabelecimentoFlow")
                .start(estabelecimentoStep)
                .build();
    }

    @Bean
    public Flow cargaHorariaCboProfissionalFlow(JobRepository repository, PlatformTransactionManager transactionManager,
                                    Step cargaHorariaCboProfissionalStep){
        return new FlowBuilder<Flow>("cargaHorariaCboProfissionalFlow")
                .start(cargaHorariaCboProfissionalStep)
                .build();
    }

    @Bean
    public Flow equipeFlow(JobRepository repository, PlatformTransactionManager transactionManager,
                                                Step equipeStep){
        return new FlowBuilder<Flow>("equipeFlow")
                .start(equipeStep)
                .build();
    }

    @Bean
    public Flow estabelecimentoEquipeProfissionalFlow(JobRepository repository, PlatformTransactionManager transactionManager,
                           Step estabelecimentoEquipeProfissionalStep){
        return new FlowBuilder<Flow>("estabelecimentoEquipeProfissionalFlow")
                .start(estabelecimentoEquipeProfissionalStep)
                .build();
    }
}