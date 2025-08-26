package com.Glebson.ETL.Utils;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties("file.download")
public class DownloadProperties {
    String cnesUrl;
    String sigtapUrl;
    String cnesCompetence;

    public String getSigtapUrl() {
        return sigtapUrl;
    }

    public void setSigtapUrl(String sigtapUrl) {
        this.sigtapUrl = sigtapUrl;
    }

    public String getCnesUrl() {
        return cnesUrl;
    }

    public void setCnesUrl(String cnesUrl) {
        this.cnesUrl = cnesUrl;
    }

    public String getCnesCompetence() {
        return cnesCompetence;
    }

    public void setCnesCompetence(String cnesCompetence) {
        this.cnesCompetence = cnesCompetence;
    }
}
