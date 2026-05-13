package edu.cugb.faft.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "faft")
@Data
public class FaftProperties {

    private double alpha;
    private double beta;
    private double gamma;
    private double rmin; // 最小采样率
    private double rmax; // 最大采样率
    private double step;
    private double errorThreshold; // 错误阈值
    private double omegaIn;  // D(v) 入度权重
    private double omegaOut; // D(v) 出度权重
    private double impactDelta;

}
