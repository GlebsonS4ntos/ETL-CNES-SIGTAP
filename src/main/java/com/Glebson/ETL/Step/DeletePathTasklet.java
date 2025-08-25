package com.Glebson.ETL.Step;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;
import org.springframework.util.FileSystemUtils;

import java.nio.file.Path;
import java.nio.file.Paths;

@Component
public class DeletePathTasklet implements Tasklet {

    private static final Logger log = LoggerFactory.getLogger(DownloadUnzipTasklet.class);
    private final Path dir = Paths.get("download");

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        FileSystemUtils.deleteRecursively(dir);
        log.info("Diretório 'download' apagado com sucesso.");
        return RepeatStatus.FINISHED;
    }
}
