package net.sparkworks.e2data;

import uk.ac.manchester.tornado.api.annotations.Parallel;
import uk.ac.manchester.tornado.api.annotations.Reduce;

import java.util.Objects;

public class AnalyticsProcessor {
    
/*
    private static AnalyticsProcessor instance;
    
    private AnalyticsProcessor() {}
    
    public static AnalyticsProcessor getInstance() {
        if (Objects.isNull(instance)) {
            instance = new AnalyticsProcessor();
        }
        return instance;
    }
*/
    
    
    
    public static void computeMin(final double[] values, final @Reduce double[] result) {
        result[0] = values[0];
        for (@Parallel int i = 1; i < values.length; i++) {
            if (values[i] < result[0]) {
                result[0] = values[i];
            }
        }
    }
    
    public static void computeMax(final double[] values, final @Reduce double[] result) {
        result[0] = values[0];
        for (@Parallel int i = 1; i < values.length; i++) {
            if (values[i] > result[0]) {
                result[0] = values[i];
            }
        }
    }
    
    public static void computeSum(final double[] values, final @Reduce double[] result) {
        result[0] = 0;
        for (@Parallel int i = 0; i < values.length; i++) {
            result[0] += values[i];
        }
    }
    
    public static void computeAvg(final double[] values, double[] result) {
        computeSum(values, result);
        result[0] = result[0] / values.length;
    }
    
    public static void computeStandardDeviation(final double values[], final @Reduce double[] result) {
        double sum = 0.0, standardDeviation = 0.0;
        int length = values.length;
        
        for (@Parallel int i = 0; i < length; i++) {
            sum += values[i];
        }
        
        double mean = sum / length;
        
        for (@Parallel int i = 0; i < length; i++) {
            standardDeviation += Math.pow(values[i] - mean, 2);
        }
        
        result[1] = Math.sqrt(standardDeviation / length);
    }
    
    public static void computeMean(final double values[], final @Reduce double[] result) {
        double sum = 0;
        for (@Parallel int i = 0; i < values.length; i++) {
            sum += values[i];
        }
        result[0] = sum / values.length;
    }
    
    public static void removeOutliers(final double values[], final @Reduce double[] result) {
        computeMean(values, result);
        computeStandardDeviation(values, result);
        double min = result[0] - (2 * result[1]);
        double max = result[0] + (2 * result[1]);
        long count = 0;
        for (@Parallel int i = 0; i < values.length; i++) {
            if (values[i] > max || values[i] < min) {
                count++;
            }
        }
        result[2] = count;
    }
    
}
