package net.sparkworks.e2data;

import uk.ac.manchester.tornado.api.TaskSchedule;

import java.util.Random;

public class AnalyticsSampleEngine {
    
    static final double LOWER_RANDOM_BOUND = 0d;
    
    static final double UPPER_RANDOM_BOUND = 150d;
    
    public static void main(String... args) {
        if (args.length == 0) {
            throw new IllegalStateException("Please provide either the number of sample values or the path of the sample csv file");
        }
        final String arg = args[0];
        if (isNumeric(arg)) {
            final double[] samples = generateRandomValuesOfSizeWithOutliers(Integer.parseInt(arg));
            executeAnalytics(arg, samples);
        } else {
            try {
                final double[] samples = CsvUtils.parseSamplesFromCsv(arg);
                executeAnalytics(arg, samples);
            } catch (Exception e) {
                System.out.println("Parsing csv file failed, please check the file format");
                e.printStackTrace();
            }
        }
    }
    
    private static void executeAnalytics(final String arg, final double[] samples) {
//        final AnalyticsProcessor analyticsProcessor = AnalyticsProcessor.getInstance();
        final double[] result = new double[1];
        TaskSchedule task0 = new TaskSchedule("s0")
                .streamIn(samples)
                .task("t0", AnalyticsProcessor::computeMin, samples, result)
                .streamOut(result);
        ExecutionTime.printTime(() -> task0.execute());
        System.out
                .println(String.format(" computing Min of %s random samples with result %f", arg, result[0]));
    
        TaskSchedule task1 = new TaskSchedule("s1")
                .streamIn(samples)
                .task("t1", AnalyticsProcessor::computeMax, samples, result)
                .streamOut(result);
        ExecutionTime.printTime(() -> task1.execute());
        System.out
                .println(String.format(" computing Max of %s random samples with result %f", arg, result[0]));
        
        TaskSchedule task2 = new TaskSchedule("s2")
                .streamIn(samples)
                .task("t2", AnalyticsProcessor::computeSum, samples, result)
                .streamOut(result);
        ExecutionTime.printTime(() -> task2.execute());
        System.out
                .println(String.format(" computing Sum of %s random samples with result %f", arg, result[0]));
    
        TaskSchedule task3 = new TaskSchedule("s3")
                .streamIn(samples)
                .task("t3", AnalyticsProcessor::computeAvg, samples, result)
                .streamOut(result);
        ExecutionTime.printTime(() -> task3.execute());
        System.out
                .println(String.format(" computing Avg of %s random samples with result %f", arg, result[0]));
    
        final double[] taskOutliersResult = new double[3];
        TaskSchedule task4 = new TaskSchedule("s4")
                .streamIn(samples)
                .task("t4", AnalyticsProcessor::removeOutliers, samples, taskOutliersResult)
                .streamOut(result);
        ExecutionTime.printTime(() -> task4.execute());
        System.out
                .println(String
                        .format(" computing Outliers of %s random samples with mean %f, standard deviation %f and outliers count %f",
                                arg, taskOutliersResult[0], taskOutliersResult[1], taskOutliersResult[2]));
    
        
        // vanilla
/*
        ExecutionTime.printTime(() -> AnalyticsProcessor.computeMin(samples, result));
        System.out
                .println(String.format(" computing Min of %s random samples with result %f", arg, result[0]));
    
        ExecutionTime.printTime(() -> AnalyticsProcessor.computeMax(samples, result));
        System.out
                .println(String.format(" computing Max of %s random samples with result %f", arg, result[0]));
    
        ExecutionTime.printTime(() -> AnalyticsProcessor.computeSum(samples, result));
        System.out
                .println(String.format(" computing Sum of %s random samples with result %f", arg, result[0]));
    
        ExecutionTime.printTime(() -> AnalyticsProcessor.computeAvg(samples, result));
        System.out
                .println(String.format(" computing Avg of %s random samples with result %f", arg, result[0]));
    
        final double[] outliersResult = new double[3];
        ExecutionTime.printTime(() -> AnalyticsProcessor.removeOutliers(samples, outliersResult));
        System.out
                .println(String.format(" computing Outliers of %s random samples with mean %f, standard deviation %f and outliers count %f", arg, outliersResult[0], outliersResult[1], outliersResult[2]));
*/
    }
    
    private static boolean isNumeric(final String arg) {
        return arg.chars().allMatch(Character::isDigit);
    }
    
    private static double[] generateRandomValuesOfSize(final long size) {
        return new Random().doubles(size, LOWER_RANDOM_BOUND, UPPER_RANDOM_BOUND).toArray();
    }
    
    private static double[] generateRandomValuesOfSizeWithOutliers(final int size) {
        final double[] doubles = new Random().doubles(size, LOWER_RANDOM_BOUND, UPPER_RANDOM_BOUND).toArray();
        doubles[(size - 2)] = UPPER_RANDOM_BOUND + (5 * UPPER_RANDOM_BOUND);
        doubles[(size - 1)] = UPPER_RANDOM_BOUND + (10 * UPPER_RANDOM_BOUND);
        return doubles;
    }
    
}
