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
        int length = values.length;
        
        double mean = result[0] / length;
        
        for (@Parallel int i = 0; i < length; i++) {
            result[1] += Math.pow(values[i] - mean, 2);
        }
    }
    
    public static void computeMean(final double values[], final @Reduce double[] result) {
        for (@Parallel int i = 0; i < values.length; i++) {
            result[0] += values[i];
        }
    }
    
    public static void removeOutliers(final double values[], final @Reduce double[] result) {
        computeMean(values, result);
        result[0] = result[0] / values.length;
        // result[0] holds the mean value now
        computeStandardDeviation(values, result);
        result[1] = Math.sqrt(result[1] / values.length);
        // result[1] holds the standard deviation now
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
    
    public static void tornadoRemoveOutliers(final double values[], final @Reduce double[] result) {
        // result[0] holds the mean value now
        // result[1] holds the standard deviation now
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
