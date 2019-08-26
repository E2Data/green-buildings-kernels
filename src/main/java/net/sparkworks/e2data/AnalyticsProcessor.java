package net.sparkworks.e2data;

import uk.ac.manchester.tornado.api.annotations.Parallel;
import uk.ac.manchester.tornado.api.annotations.Reduce;
import uk.ac.manchester.tornado.api.collections.math.TornadoMath;

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
    
    
    
    public static void computeMin(final double[] values, @Reduce double[] result) {
        for (@Parallel int i = 0; i < values.length; i++) {
            result[0] = Math.min(result[0], values[i]);
        }
    }
    
    public static void computeMax(final double[] values, @Reduce double[] result) {
        for (@Parallel int i = 0; i < values.length; i++) {
            result[0] = Math.max(result[0], values[i]);
        }
    }
    
    public static void computeSum(final double[] values, @Reduce double[] result) {
        result[0] = 0;
        for (@Parallel int i = 0; i < values.length; i++) {
            result[0] += values[i];
        }
    }
    
    public static void computeAvg(final double[] values, @Reduce double[] result, double[] avgResult) {
        result[0] = 0;
        for (@Parallel int i = 0; i < values.length; i++) {
            result[0] += values[i];
        }
        avgResult[0] = result[0] / values.length;
    }
    
    public static void computeStandardDeviation(final double values[], double[] result) {
        int length = values.length;
        result[0] = result[0] / length;
        
        for (@Parallel int i = 0; i < length; i++) {
            result[1] += Math.pow(values[i] - result[0], 2);
        }
    }
    
    public static void prepareTornadoSumForMeanComputation(final double values[], @Reduce double[] result) {
        for (@Parallel int i = 0; i < values.length; i++) {
            result[0] += values[i];
        }
    }
    
    public static void removeOutliers(final double values[], @Reduce double[] result) {
        prepareTornadoSumForMeanComputation(values, result);
        result[0] = result[0] / values.length;
        // result[0] holds the mean value now
        computeStandardDeviation(values, result);
        result[1] = Math.sqrt(result[1] / values.length);
        // result[1] holds the stabdard deviation now
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
    
    public static void tornadoRemoveOutliers(final double values[], double[] result) {
        //result[1] = TornadoMath.sqrt(result[1] / values.length);
        result[1] = Math.sqrt(result[1] / values.length);
        // result[0] holds the mean value now
        // result[1] holds the stabdard deviation now
        double min = result[0] - (2 * result[1]);
        double max = result[0] + (2 * result[1]);
        result[2] = 0;
        long count = 0;
        for (@Parallel int i = 0; i < values.length; i++) {
            if (values[i] > max || values[i] < min) {
                result[2]++;
            }
        }
    }
    
}
