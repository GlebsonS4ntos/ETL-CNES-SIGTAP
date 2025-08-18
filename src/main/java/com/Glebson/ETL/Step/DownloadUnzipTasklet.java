package com.Glebson.ETL.Step;

import com.Glebson.ETL.Utils.DownloadProperties;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Component
public class DownloadUnzipTasklet implements Tasklet {

    private final DownloadProperties downloadProperties;
    private static final Logger log = LoggerFactory.getLogger(DownloadUnzipTasklet.class);
    private final Path dir = Paths.get("download"); // corrigido nome

    public DownloadUnzipTasklet(DownloadProperties downloadProperties) {
        this.downloadProperties = downloadProperties;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        log.info("Step de download iniciado.");
        Files.createDirectories(dir);

        CompletableFuture<String> cnesDownload = downloadArchiveAndUnzip(downloadProperties.getCnesUrl());
        CompletableFuture<String> sigtapDownload = downloadArchiveAndUnzip(downloadProperties.getSigtapUrl());

        CompletableFuture.allOf(cnesDownload, sigtapDownload).join();

        chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext().put("pathSigtap", sigtapDownload.get());
        chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext().put("pathCnes", cnesDownload.get());

        return RepeatStatus.FINISHED;
    }

    private CompletableFuture<String> downloadArchiveAndUnzip(String url) {
        return CompletableFuture.supplyAsync(() -> {
            log.info("Download do arquivo iniciado a partir da seguinte url: {}", url);
            try {
                String zipFilePath = url.startsWith("ftp") ? downloadFromFtp(url) : downloadFromHttp(url);
                return unzipFile(zipFilePath);
            } catch (IOException | URISyntaxException e) {
                log.error("Erro no download/descompactação do arquivo: {}", url, e);
                throw new RuntimeException(e);
            }
        });
    }

    private String downloadFromFtp(String ftpUrl) throws IOException {
        URI uri = URI.create(ftpUrl);

        String remoteFile = uri.getPath();
        String fileName = Paths.get(remoteFile).getFileName().toString();

        String server = uri.getHost();
        int port = uri.getPort() == -1 ? 21 : uri.getPort();

        FTPClient ftpClient = new FTPClient();
        Path localFilePath = dir.resolve(fileName); // usa a pasta download
        try {
            ftpClient.connect(server, port);
            ftpClient.login("anonymous", "");
            ftpClient.enterLocalPassiveMode();
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

            try (OutputStream outputStream = new FileOutputStream(localFilePath.toFile())) {
                boolean success = ftpClient.retrieveFile(remoteFile, outputStream);
                if (!success) {
                    throw new IOException("Falha ao baixar arquivo via FTP: " + remoteFile);
                }
            }

            log.info("Arquivo baixado com sucesso na seguinte url: {}", ftpUrl);
        } finally {
            if (ftpClient.isConnected()) {
                ftpClient.logout();
                ftpClient.disconnect();
            }
        }
        return localFilePath.toString();
    }

    private String downloadFromHttp(String fileUrl) throws IOException, URISyntaxException {
        String fileName;
        Path localFilePath;

        try {
            URI uri = new URI(fileUrl);
            URL url = uri.toURL();

            String[] queryParts = url.getQuery().split("=");
            fileName = queryParts[1];
            localFilePath = dir.resolve(fileName); // salva dentro de "download"

            try (ReadableByteChannel rbc = Channels.newChannel(url.openStream());
                 FileOutputStream fos = new FileOutputStream(localFilePath.toFile())) {

                fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
            }

            log.info("Arquivo baixado com sucesso na seguinte url: {}", fileUrl);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return localFilePath.toString();
    }

    private String unzipFile(String zipFilePath) throws IOException {
        Path zipPath = Paths.get(zipFilePath);
        String baseName = zipPath.getFileName().toString().replaceAll("(?i)\\.zip$", "");
        Path outputPath = dir.resolve(baseName); // descompacta dentro da pasta download

        int counter = 1;
        while (Files.exists(outputPath)) {
            outputPath = dir.resolve(baseName + "_" + counter);
            counter++;
        }

        Files.createDirectories(outputPath);

        try (ZipInputStream zis = new ZipInputStream(new FileInputStream(zipPath.toFile()))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                Path newFilePath = outputPath.resolve(entry.getName());
                if (entry.isDirectory()) {
                    Files.createDirectories(newFilePath);
                } else {
                    Files.createDirectories(newFilePath.getParent());
                    try (FileOutputStream fos = new FileOutputStream(newFilePath.toFile())) {
                        byte[] buffer = new byte[4096];
                        int len;
                        while ((len = zis.read(buffer)) > 0) {
                            fos.write(buffer, 0, len);
                        }
                    }
                }
                zis.closeEntry();
            }
        }

        log.info("Arquivo descompactado com sucesso: {}", outputPath);
        return outputPath.toString();
    }
}