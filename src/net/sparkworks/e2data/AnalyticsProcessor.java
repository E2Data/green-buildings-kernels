package net.sparkworks.e2data;

import java.util.Objects;

public class AnalyticsProcessor {
    
    private static AnalyticsProcessor instance;
    
    private AnalyticsProcessor() {}
    
    public static AnalyticsProcessor getInstance() {
        if (Objects.isNull(instance)) {
            instance = new AnalyticsProcessor();
        }
        return instance;
    }
    
    
    
    public double computeMin(final double[] values) {
        if (Objects.isNull(values) || values.length == 0) {
            throw new IllegalStateException();
        }
        double min = values[0];
        for (int i = 1; i < values.length; i++) {
            if (values[i] < min) {
                min = values[i];
            }
        }
        return min;
    }
    
    public double computeMax(final double[] values) {
        if (Objects.isNull(values) || values.length == 0) {
            throw new IllegalStateException();
        }
        double max = values[0];
        for (int i = 1; i < values.length; i++) {
            if (values[i] > max) {
                max = values[i];
            }
        }
        return max;
    }
    
    public double computeSum(final double[] values) {
        if (Objects.isNull(values) || values.length == 0) {
            throw new IllegalStateException();
        }
        double sum = 0;
        for (int i = 0; i < values.length; i++) {
            sum += values[i];
        }
        return sum;
    }
    
    public double computeAvg(final double[] values) {
        return computeSum(values) / values.length;
    }
    
}
