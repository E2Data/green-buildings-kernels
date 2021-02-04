package net.sparkworks.e2data;

import uk.ac.manchester.tornado.api.annotations.Parallel;
import uk.ac.manchester.tornado.api.annotations.Reduce;
import uk.ac.manchester.tornado.api.collections.math.TornadoMath;

public class AnalyticsProcessor {
    
    public static void computeMin(final float[] values, @Reduce float[] result) {
        for (@Parallel int i = 0; i < values.length; i++) {
            result[0] = Math.min(result[0], values[i]);
        }
    }
    
    public static void computeMax(final float[] values, @Reduce float[] result) {
        for (@Parallel int i = 0; i < values.length; i++) {
            result[0] = Math.max(result[0], values[i]);
        }
    }
    
    public static void computeSum(final float[] values, @Reduce float[] result) {
        for (@Parallel int i = 0; i < values.length; i++) {
            result[0] += values[i];
        }
    }
    
    public static void prepareSumForAvg(final float[] values, @Reduce float[] result) {
        for (@Parallel int i = 0; i < values.length; i++) {
            result[0] += values[i];
        }
    }
    
    public static void computeAvg(final float[] values, float[] result) {
        result[0] = result[0] / values.length;
    }
    
    public static void computeStandardDeviation(final float values[], float[] result) {
        int length = values.length;
        result[0] = result[0] / length;
        
        for (@Parallel int i = 0; i < length; i++) {
            result[1] += Math.pow(values[i] - result[0], 2);
        }
    }
    
    public static void prepareTornadoSumForMeanComputation(final float values[], @Reduce float[] result) {
        for (@Parallel int i = 0; i < values.length; i++) {
            result[0] += values[i];
        }
    }
    
    public static void removeOutliers(final float values[], @Reduce float[] result) {
        prepareTornadoSumForMeanComputation(values, result);
        result[0] = result[0] / values.length;
        // result[0] holds the mean value now
        computeStandardDeviation(values, result);
        result[1] = TornadoMath.sqrt(result[1] / values.length);
        // result[1] holds the stabdard deviation now
        float min = result[0] - (2 * result[1]);
        float max = result[0] + (2 * result[1]);
        long count = 0;
        for (@Parallel int i = 0; i < values.length; i++) {
            if (values[i] > max || values[i] < min) {
                count++;
            }
        }
        result[2] = count;
    }
    
    public static void tornadoRemoveOutliers(final float values[], float[] result) {
        //result[1] = TornadoMath.sqrt(result[1] / values.length);
        result[1] = TornadoMath.sqrt(result[1] / values.length);
        // result[0] holds the mean value now
        // result[1] holds the stabdard deviation now
        float min = result[0] - (2 * result[1]);
        float max = result[0] + (2 * result[1]);
        result[2] = 0;
        long count = 0;
        for (@Parallel int i = 0; i < values.length; i++) {
            if (values[i] > max || values[i] < min) {
                result[2]++;
            }
        }
    }
    
}
