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
        double[] sumResult = new double[0];
        computeSum(values, sumResult);
        result[0] = sumResult[0] / values.length;
    }
    
}
