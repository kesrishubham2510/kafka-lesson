package com.myreflectionthoughts.streamer.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "producer")
public class ProducerConfig {

    private Bootstrap bootstrap = new Bootstrap();
    private int retries;

    public static class Bootstrap {
        private String servers;

        public void setServers(String servers) {
            this.servers = servers;
        }

        public String getServers() {
            return servers;
        }
    }

    public Bootstrap getBootstrap() {
        return bootstrap;
    }

    public int getRetries() {
        return retries;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }
}
