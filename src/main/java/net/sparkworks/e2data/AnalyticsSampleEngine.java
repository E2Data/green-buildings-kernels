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
    
    private static void warmUp(TaskSchedule taskSchedule) {
        for (int i = 0; i < 50; i++) {
            taskSchedule.execute();
        }
    }
    
    private static void executeAnalytics(final String arg, final double[] samples) {
//        final AnalyticsProcessor analyticsProcessor = AnalyticsProcessor.getInstance();
        final double[] result = new double[1];
    
        final double[] jvmresult = new double[1];
        
        TaskSchedule task0 = new TaskSchedule("s0")
                .streamIn(samples)
                .task("t0", AnalyticsProcessor::computeMin, samples, result)
                .streamOut(result);
        task0.warmup();
        //warmUp(task0);
        task0.execute();
        System.out
                .println(String.format("TornadoVM computing Min of %s random samples with result %f", arg, result[0]));
        
        ExecutionTime.printTime(() -> AnalyticsProcessor.computeMin(samples, jvmresult));
        System.out
                .println(String.format(" JVM computing Min of %s random samples with result %f", arg, jvmresult[0]));
        
        TaskSchedule task1 = new TaskSchedule("s1")
                .streamIn(samples)
                .task("t1", AnalyticsProcessor::computeMax, samples, result)
                .streamOut(result);
        task1.warmup();
        //warmUp(task1);
        task1.execute();
        System.out
                .println(String.format("TornadoVM computing Max of %s random samples with result %f", arg, result[0]));
        
        ExecutionTime.printTime(() -> AnalyticsProcessor.computeMax(samples, jvmresult));
        System.out
                .println(String.format(" JVM computing Max of %s random samples with result %f", arg, jvmresult[0]));
        
        TaskSchedule task2 = new TaskSchedule("s2")
                .streamIn(samples)
                .task("t2", AnalyticsProcessor::computeSum, samples, result)
                .streamOut(result);
        task2.warmup();
        //warmUp(task2);
        task2.execute();
        System.out
                .println(String.format("TornadoVM computing Sum of %s random samples with result %f", arg, result[0]));
        
        ExecutionTime.printTime(() -> AnalyticsProcessor.computeSum(samples, jvmresult));
        System.out
                .println(String.format(" JVM computing Sum of %s random samples with result %f", arg, jvmresult[0]));
    
        double[] avgResult = new double[] {-1};
        TaskSchedule task3 = new TaskSchedule("s3")
                .streamIn(samples)
                .task("t3", AnalyticsProcessor::computeAvg, samples, result, avgResult)
                .streamOut(avgResult);
        task3.warmup();
        //warmUp(task3);
        task3.execute();
        System.out
                .println(String.format("TornadoVM computing Avg of %s random samples with result %f", arg, avgResult[0]));
        
        double[] finalAvgResult = new double[] {-1};
        ExecutionTime.printTime(() -> AnalyticsProcessor.computeAvg(samples, jvmresult, finalAvgResult));
        System.out
                .println(String.format(" JVM computing Avg of %s random samples with result %f", arg, finalAvgResult[0]));
    
        final double[] taskOutliersResult = new double[3];
        TaskSchedule task41 = new TaskSchedule("s41")
                .streamIn(samples)
                .task("t4.1", AnalyticsProcessor::prepareTornadoSumForMeanComputation, samples, taskOutliersResult)
                .task("t4.2", AnalyticsProcessor::computeStandardDeviation, samples, taskOutliersResult)
                .task("t4.3", AnalyticsProcessor::tornadoRemoveOutliers, samples, taskOutliersResult)
                .streamOut(taskOutliersResult);
    
/*
        TaskSchedule task42 = new TaskSchedule("s42")
                .streamIn(samples, taskOutliersResult)
                .task("t4.3", AnalyticsProcessor::tornadoRemoveOutliers, samples, taskOutliersResult)
                .streamOut(taskOutliersResult);
*/
        //        task41.execute();
//        taskOutliersResult[0] = taskOutliersResult[0] / samples.length;
        
/*
        TaskSchedule task42 = new TaskSchedule("s42")
                .streamIn(samples)
                .task("t4.2", AnalyticsProcessor::computeStandardDeviation, samples, taskOutliersResult)
                .streamOut(taskOutliersResult);
        task42.execute();
        taskOutliersResult[1] = Math.sqrt(taskOutliersResult[1] / samples.length);
        
        TaskSchedule task4 = new TaskSchedule("s43")
                .streamIn(samples)
                .task("t4.3", AnalyticsProcessor::tornadoRemoveOutliers, samples, taskOutliersResult)
                .streamOut(taskOutliersResult);
        
        task4.warmup();
*/
    
        final double[] jvmOutliersResult = new double[3];
        ExecutionTime.printTime(() -> {
            AnalyticsProcessor.prepareTornadoSumForMeanComputation(samples, jvmOutliersResult);
            AnalyticsProcessor.computeStandardDeviation(samples, jvmOutliersResult);
            AnalyticsProcessor.tornadoRemoveOutliers(samples, jvmOutliersResult);
        });
        System.out
                .println(String
                        .format(" JVM computing Outliers of %s random samples with mean %f, standard deviation %f and outliers count %f",
                                arg, jvmOutliersResult[0], jvmOutliersResult[1], jvmOutliersResult[2]));
        
        task41.execute();
        System.out
                .println(String
                        .format("TornadoVM computing Outliers of %s random samples with mean %f, standard deviation %f and outliers count %f",
                                arg, taskOutliersResult[0], taskOutliersResult[1], taskOutliersResult[2]));
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
