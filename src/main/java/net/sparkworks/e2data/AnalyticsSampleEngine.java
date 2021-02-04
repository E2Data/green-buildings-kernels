package net.sparkworks.e2data;

import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.api.common.TornadoDevice;
import uk.ac.manchester.tornado.api.runtime.TornadoRuntime;

import java.util.Random;
import java.util.stream.IntStream;

public class AnalyticsSampleEngine {
    
    public static final int WARMING_UP_ITERATIONS = 15;
    
    public static void main(String... args) {
        if (args.length == 0) {
            throw new IllegalStateException("Please provide either the number of sample values or the path of the sample csv file");
        }
        final String arg = args[0];
        if (isNumeric(arg)) {
            final float[] samples = generateRandomValuesOfSizeWithOutliers(Integer.parseInt(arg));
            executeAnalytics(samples, args);
        } else {
            throw new RuntimeException("Please provide proper data size");
        }
    }
    
    private static void executeAnalytics(final float[] samples, final String... args) {
        
        final float[] result = new float[1];
        
        final float[] jvmresult = new float[1];
    
        TornadoDevice device = TornadoRuntime.getTornadoRuntime().getDriver(0).getDevice(0);
        System.out.println("TornadoVM on Device: " + device.getDeviceName());
        
        TaskSchedule task0 = new TaskSchedule("s0")
                .streamIn(samples)
                .task("t0", AnalyticsProcessor::computeMin, samples, result)
                .streamOut(result);
        task0.mapAllTo(device);
        
        for (int i = 0; i < WARMING_UP_ITERATIONS; i++) {
            task0.execute();
        }
        
        long start = System.nanoTime();
        task0.execute();
        long end = System.nanoTime();
        
        for (int i = 0; i < WARMING_UP_ITERATIONS; i++) {
            AnalyticsProcessor.computeMin(samples, jvmresult);
        }
        
        long startSequential = System.nanoTime();
        AnalyticsProcessor.computeMin(samples, jvmresult);
        long endSequential = System.nanoTime();
    
        System.out.println("\tMIN Kernel CPU Execution: Total time = " + (endSequential - startSequential) + " ns");
        System.out.println("\tMIN Kernel GPU Execution: Total Time = " + (end - start) + " ns");
        System.out.println("\tMIN Kernel Speedup: " + ((endSequential - startSequential) / (end - start)) + "x");
        
        result[0] = 0;
        jvmresult[0] = 0;
    
        start = 0;
        end = 0;
        startSequential = 0;
        endSequential = 0;
        
        TaskSchedule task1 = new TaskSchedule("s1")
                .streamIn(samples)
                .task("t1", AnalyticsProcessor::computeMax, samples, result)
                .streamOut(result);
        task1.mapAllTo(device);
        task1.warmup();
        
        for (int i = 0; i < WARMING_UP_ITERATIONS; i++) {
            task1.execute();
        }
        
        start = System.nanoTime();
        task1.execute();
        end = System.nanoTime();
    
    
        for (int i = 0; i < WARMING_UP_ITERATIONS; i++) {
            AnalyticsProcessor.computeMax(samples, jvmresult);
        }
    

        startSequential = System.nanoTime();
        AnalyticsProcessor.computeMax(samples, jvmresult);
        endSequential = System.nanoTime();
    

        System.out.println("\tMAX Kernel CPU Execution:  Total time = " + (endSequential - startSequential) + " ns");
        System.out.println("\tMAX Kernel GPU Execution: Total Time = " + (end - start) + " ns");
        System.out.println("\tMAX Kernel Speedup: " + ((endSequential - startSequential) / (end - start)) + "x");
        
        result[0] = 0;
        jvmresult[0] = 0;
    
        start = 0;
        end = 0;
        startSequential = 0;
        endSequential = 0;
    
        TaskSchedule task2 = new TaskSchedule("s2")
                .streamIn(samples)
                .task("t2", AnalyticsProcessor::computeSum, samples, result)
                .streamOut(result);
        task2.mapAllTo(device);
        
        for (int i = 0; i < WARMING_UP_ITERATIONS; i++) {
            task2.execute();
        }
    
        
        start = System.nanoTime();
        task2.execute();
        end = System.nanoTime();
    
    
        
        for (int i = 0; i < WARMING_UP_ITERATIONS; i++) {
            AnalyticsProcessor.computeSum(samples, jvmresult);
        }
    
        
        startSequential = System.nanoTime();
        AnalyticsProcessor.computeSum(samples, jvmresult);
        endSequential = System.nanoTime();
    
        System.out.println("\tSUM Kernel CPU Execution: Total time = " + (endSequential - startSequential) + " ns");
        System.out.println("\tSUM Kernel GPU Execution: Total Time = " + (end - start) + " ns");
        System.out.println("\tSUM Kernel Speedup: " + ((endSequential - startSequential) / (end - start)) + "x");
    
        
        result[0] = 0;
        jvmresult[0] = 0;
    
        start = 0;
        end = 0;
        startSequential = 0;
        endSequential = 0;
        
        TaskSchedule task3 = new TaskSchedule("s3")
                .streamIn(samples)
                .task("t3.1", AnalyticsProcessor::prepareSumForAvg, samples, result)
                .task("t3.2", AnalyticsProcessor::computeAvg, samples, result)
                .streamOut(result);
        task3.mapAllTo(device);
        for (int i = 0; i < WARMING_UP_ITERATIONS; i++) {
            task3.execute();
        }
        
        start = System.nanoTime();
        task3.execute();
        end = System.nanoTime();
    
    
        for (int i = 0; i < WARMING_UP_ITERATIONS; i++) {
            AnalyticsProcessor.prepareSumForAvg(samples, jvmresult);
            AnalyticsProcessor.computeAvg(samples, jvmresult);
            jvmresult[0] = 0;
        }
        startSequential = System.nanoTime();
        AnalyticsProcessor.prepareSumForAvg(samples, jvmresult);
        AnalyticsProcessor.computeAvg(samples, jvmresult);
        jvmresult[0] = 0;
        endSequential = System.nanoTime();
    
        System.out.println("\tAVG Kernel CPU Execution: Total time = " + (endSequential - startSequential) + " ns");
        System.out.println("\tAVG Kernel GPU Execution: Total Time = " + (end - start) + " ns");
        System.out.println("\tAVG Kernel Speedup: " + ((endSequential - startSequential) / (end - start)) + "x");
    
        start = 0;
        end = 0;
        startSequential = 0;
        endSequential = 0;
    
        int size = samples.length;
        int window_size = 60;
        
        if (args.length >= 2) {
            try {
                window_size = Integer.parseInt(args[1]);
            } catch (NumberFormatException nfe) {
                window_size = 60;
            }
        }
    
        System.out.println("Computing outliers, min, max, average for sensor stream of " + size + " elements with window size " + window_size);
    
        float[] rawValues = new float[size];
        float[] windowAverage = new float[size];
        float[] windowMin = new float[size];
        float[] windowMax = new float[size];
        float[] outliers = new float[size];
    
    
        Random r = new Random();
        IntStream.range(0, size).parallel().forEach(idx -> {
            rawValues[idx] = r.nextFloat();
        });
    

        TaskSchedule t = new TaskSchedule("s0")
                .task("t0", WindowOperations::matrixMultiplication, rawValues, windowMin, windowMax, windowAverage, outliers, size, window_size)
                .streamOut(outliers);

        t.mapAllTo(device);

        for (int i = 0; i < WARMING_UP_ITERATIONS; i++) {
            t.execute();
        }
    

        start = System.nanoTime();
        t.execute();
        end = System.nanoTime();
    
        for (int i = 0; i < WARMING_UP_ITERATIONS; i++) {
            WindowOperations.matrixMultiplication(rawValues, windowMin, windowMax, windowAverage, outliers, size, window_size);
        }
    

        startSequential = System.nanoTime();
        WindowOperations.matrixMultiplication(rawValues, windowMin, windowMax, windowAverage, outliers, size, window_size);
        endSequential = System.nanoTime();
        
        System.out.println("\tOutliers Kernel CPU Execution: Total time = " + (endSequential - startSequential) + " ns");
        System.out.println("\tOutliers Kernel GPU Execution: Total Time = " + (end - start) + " ns");
        System.out.println("\tOutliers Kernel Speedup: " + ((endSequential - startSequential) / (end - start)) + "x");
    
    
    }
    
    private static boolean isNumeric(final String arg) {
        return arg.chars().allMatch(Character::isDigit);
    }
    
    private static float[] generateRandomValuesOfSizeWithOutliers(final int size) {
        float[] randoms = new float[size];
        Random r = new Random();
        for (int i = 0; i < size; i++) {
            randoms[i] = r.nextFloat();
        }
        return randoms;
    }
    
}
