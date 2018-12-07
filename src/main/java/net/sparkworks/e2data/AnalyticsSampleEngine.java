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
            final double[] samples = generateRandomValuesOfSize(Long.parseLong(arg));
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
        final AnalyticsProcessor analyticsProcessor = AnalyticsProcessor.getInstance();
        final double[] result = null;
        TaskSchedule task = new TaskSchedule("s0")
                .streamIn(samples)
                .task("t0", analyticsProcessor::computeMin, samples, result)
                .streamOut(result);
        ExecutionTime.printTime(() -> task.execute());
        System.out
                .println(String.format(" computing Min of %s random samples with result %f", arg, result[0]));
/*
        System.out
                .println(String.format("Max of %s random samples is %f", arg,
                        ExecutionTime.printTime(() -> analyticsProcessor.computeMax(samples))));
        System.out
                .println(String.format("Sum of %s random samples is %f", arg,
                        ExecutionTime.printTime(() -> analyticsProcessor.computeSum(samples))));
        System.out
                .println(String.format("Avg of %s random samples is %f", arg,
                        ExecutionTime.printTime(() -> analyticsProcessor.computeAvg(samples))));
*/
    }
    
    private static boolean isNumeric(final String arg) {
        return arg.chars().allMatch(Character::isDigit);
    }
    
    private static double[] generateRandomValuesOfSize(final long size) {
        return new Random().doubles(size, LOWER_RANDOM_BOUND, UPPER_RANDOM_BOUND).toArray();
    }
    
}
