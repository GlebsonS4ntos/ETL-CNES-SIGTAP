package com.Glebson.ETL.Utils;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties("file.download")
public record DownloadProperties (
    String cnesUrl,
    String sigtapUrl ){}
