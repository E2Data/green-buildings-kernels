package net.sparkworks.e2data;

import uk.ac.manchester.tornado.api.TaskSchedule;

import java.util.Random;
import java.util.stream.IntStream;

import static net.sparkworks.e2data.WindowOperations.matrixMultiplication;

public class AnalyticsSampleEngine {
    
    static final double LOWER_RANDOM_BOUND = 0d;
    
    static final double UPPER_RANDOM_BOUND = 150d;
    
    public static final int WARMING_UP_ITERATIONS = 15;
    
    public static void main(String... args) {
        if (args.length == 0) {
            throw new IllegalStateException("Please provide either the number of sample values or the path of the sample csv file");
        }
        final String arg = args[0];
        if (isNumeric(arg)) {
            final double[] samples = generateRandomValuesOfSizeWithOutliers(Integer.parseInt(arg));
            executeAnalytics(samples, args);
        } else {
            try {
                final double[] samples = CsvUtils.parseSamplesFromCsv(arg);
                executeAnalytics(samples, args);
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
    
    private static void executeAnalytics(final double[] samples, final String... args) {
        
        final double[] result = new double[1];
        
        final double[] jvmresult = new double[1];
        
        TaskSchedule task0 = new TaskSchedule("s0")
                .streamIn(samples)
                //                .batch("2GB")
                .task("t0", AnalyticsProcessor::computeMin, samples, result)
                .streamOut(result);
    
        // 1. Warm up Tornado
        for (int i = 0; i < WARMING_UP_ITERATIONS; i++) {
            task0.execute();
        }
    
        // 2. Run parallel on the GPU with Tornado
        long start = System.currentTimeMillis();
        task0.execute();
        long end = System.currentTimeMillis();
    
    
        // Run sequential
        // 1. Warm up sequential
        for (int i = 0; i < WARMING_UP_ITERATIONS; i++) {
            AnalyticsProcessor.computeMin(samples, jvmresult);
        }
    
        // 2. Run the sequential code
        long startSequential = System.currentTimeMillis();
        AnalyticsProcessor.computeMin(samples, jvmresult);
        long endSequential = System.currentTimeMillis();
    
        // Compute Gigaflops and performance
        long msecGPUElapsedTime = (end - start);
        long msecCPUElaptedTime = (endSequential - startSequential);
        double flops = 2 * Math.pow(samples.length, 3);
        double gpuGigaFlops = (1.0E-9 * flops) / (msecGPUElapsedTime / 1000.0f);
        double cpuGigaFlops = (1.0E-9 * flops) / (msecCPUElaptedTime / 1000.0f);
    
        String formatGPUFGlops = String.format("%.2f", gpuGigaFlops);
        String formatCPUFGlops = String.format("%.2f", cpuGigaFlops);
    
        System.out.println("\tMIN Kernel CPU Execution: " + formatCPUFGlops + " GFlops, Total time = " + (endSequential - startSequential) + " ms");
        System.out.println("\tMIN Kernel GPU Execution: " + formatGPUFGlops + " GFlops, Total Time = " + (end - start) + " ms");
        System.out.println("\tMIN Kernel Speedup: " + ((endSequential - startSequential) / (end - start)) + "x");
        
        result[0] = 0;
        jvmresult[0] = 0;
    
        start = 0;
        end = 0;
        startSequential = 0;
        endSequential = 0;
        
        TaskSchedule task1 = new TaskSchedule("s1")
                .streamIn(samples)
                //                .batch("2GB")
                .task("t1", AnalyticsProcessor::computeMax, samples, result)
                .streamOut(result);
        task1.warmup();
        
        // 1. Warm up Tornado
        for (int i = 0; i < WARMING_UP_ITERATIONS; i++) {
            task1.execute();
        }
    
        // 2. Run parallel on the GPU with Tornado
        start = System.currentTimeMillis();
        task1.execute();
        end = System.currentTimeMillis();
    
    
        // Run sequential
        // 1. Warm up sequential
        for (int i = 0; i < WARMING_UP_ITERATIONS; i++) {
            AnalyticsProcessor.computeMax(samples, jvmresult);
        }
    
        // 2. Run the sequential code
        startSequential = System.currentTimeMillis();
        AnalyticsProcessor.computeMax(samples, jvmresult);
        endSequential = System.currentTimeMillis();
    
        // Compute Gigaflops and performance
        msecGPUElapsedTime = (end - start);
        msecCPUElaptedTime = (endSequential - startSequential);
        flops = 2 * Math.pow(samples.length, 3);
        gpuGigaFlops = (1.0E-9 * flops) / (msecGPUElapsedTime / 1000.0f);
        cpuGigaFlops = (1.0E-9 * flops) / (msecCPUElaptedTime / 1000.0f);
    
        formatGPUFGlops = String.format("%.2f", gpuGigaFlops);
        formatCPUFGlops = String.format("%.2f", cpuGigaFlops);
    
        System.out.println("\tMAX Kernel CPU Execution: " + formatCPUFGlops + " GFlops, Total time = " + (endSequential - startSequential) + " ms");
        System.out.println("\tMAX Kernel GPU Execution: " + formatGPUFGlops + " GFlops, Total Time = " + (end - start) + " ms");
        System.out.println("\tMAX Kernel Speedup: " + ((endSequential - startSequential) / (end - start)) + "x");
        
        result[0] = 0;
        jvmresult[0] = 0;
    
        start = 0;
        end = 0;
        startSequential = 0;
        endSequential = 0;
    
        TaskSchedule task2 = new TaskSchedule("s2")
                .streamIn(samples)
                //                .batch("2GB")
                .task("t2", AnalyticsProcessor::computeSum, samples, result)
                .streamOut(result);
    
        // 1. Warm up Tornado
        for (int i = 0; i < WARMING_UP_ITERATIONS; i++) {
            task2.execute();
        }
    
        // 2. Run parallel on the GPU with Tornado
        start = System.currentTimeMillis();
        task2.execute();
        end = System.currentTimeMillis();
    
    
        // Run sequential
        // 1. Warm up sequential
        for (int i = 0; i < WARMING_UP_ITERATIONS; i++) {
            AnalyticsProcessor.computeSum(samples, jvmresult);
        }
    
        // 2. Run the sequential code
        startSequential = System.currentTimeMillis();
        AnalyticsProcessor.computeSum(samples, jvmresult);
        endSequential = System.currentTimeMillis();
    
        // Compute Gigaflops and performance
        msecGPUElapsedTime = (end - start);
        msecCPUElaptedTime = (endSequential - startSequential);
        flops = 2 * Math.pow(samples.length, 3);
        gpuGigaFlops = (1.0E-9 * flops) / (msecGPUElapsedTime / 1000.0f);
        cpuGigaFlops = (1.0E-9 * flops) / (msecCPUElaptedTime / 1000.0f);
    
        formatGPUFGlops = String.format("%.2f", gpuGigaFlops);
        formatCPUFGlops = String.format("%.2f", cpuGigaFlops);
    
        System.out.println("\tSUM Kernel CPU Execution: " + formatCPUFGlops + " GFlops, Total time = " + (endSequential - startSequential) + " ms");
        System.out.println("\tSUM Kernel GPU Execution: " + formatGPUFGlops + " GFlops, Total Time = " + (end - start) + " ms");
        System.out.println("\tSUM Kernel Speedup: " + ((endSequential - startSequential) / (end - start)) + "x");
    
        
        result[0] = 0;
        jvmresult[0] = 0;
    
        start = 0;
        end = 0;
        startSequential = 0;
        endSequential = 0;
        
        TaskSchedule task3 = new TaskSchedule("s3")
                .streamIn(samples)
                //                .batch("2GB")
                .task("t3.1", AnalyticsProcessor::prepareSumForAvg, samples, result)
                .task("t3.2", AnalyticsProcessor::computeAvg, samples, result)
                .streamOut(result);
    
        // 1. Warm up Tornado
        for (int i = 0; i < WARMING_UP_ITERATIONS; i++) {
            task3.execute();
        }
    
        // 2. Run parallel on the GPU with Tornado
        start = System.currentTimeMillis();
        task3.execute();
        end = System.currentTimeMillis();
    
    
        // Run sequential
        // 1. Warm up sequential
        for (int i = 0; i < WARMING_UP_ITERATIONS; i++) {
            AnalyticsProcessor.prepareSumForAvg(samples, jvmresult);
            AnalyticsProcessor.computeAvg(samples, jvmresult);
            jvmresult[0] = 0;
        }
    
        // 2. Run the sequential code
        startSequential = System.currentTimeMillis();
        AnalyticsProcessor.prepareSumForAvg(samples, jvmresult);
        AnalyticsProcessor.computeAvg(samples, jvmresult);
        jvmresult[0] = 0;
        endSequential = System.currentTimeMillis();
    
        // Compute Gigaflops and performance
        msecGPUElapsedTime = (end - start);
        msecCPUElaptedTime = (endSequential - startSequential);
        flops = 2 * Math.pow(samples.length, 3);
        gpuGigaFlops = (1.0E-9 * flops) / (msecGPUElapsedTime / 1000.0f);
        cpuGigaFlops = (1.0E-9 * flops) / (msecCPUElaptedTime / 1000.0f);
    
        formatGPUFGlops = String.format("%.2f", gpuGigaFlops);
        formatCPUFGlops = String.format("%.2f", cpuGigaFlops);
    
        System.out.println("\tAVG Kernel CPU Execution: " + formatCPUFGlops + " GFlops, Total time = " + (endSequential - startSequential) + " ms");
        System.out.println("\tAVG Kernel GPU Execution: " + formatGPUFGlops + " GFlops, Total Time = " + (end - start) + " ms");
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
    
        //@formatter:off
        TaskSchedule t = new TaskSchedule("s0")
                .task("t0", net.sparkworks.e2data.WindowOperations::matrixMultiplication, rawValues, windowMin, windowMax, windowAverage, outliers, size, window_size)
                .streamOut(outliers);
        //@formatter:on
    
        // 1. Warm up Tornado
        for (int i = 0; i < WARMING_UP_ITERATIONS; i++) {
            t.execute();
        }
    
        // 2. Run parallel on the GPU with Tornado
        start = System.currentTimeMillis();
        t.execute();
        end = System.currentTimeMillis();
    
        // Run sequential
        // 1. Warm up sequential
        for (int i = 0; i < WARMING_UP_ITERATIONS; i++) {
            matrixMultiplication(rawValues, windowMin, windowMax, windowAverage, outliers, size, window_size);
        }
    
        // 2. Run the sequential code
        startSequential = System.currentTimeMillis();
        matrixMultiplication(rawValues, windowMin, windowMax, windowAverage, outliers, size, window_size);
        endSequential = System.currentTimeMillis();
    
        // Compute Gigaflops and performance
        msecGPUElapsedTime = (end - start);
        msecCPUElaptedTime = (endSequential - startSequential);
        flops = 2 * Math.pow(size, 3);
        gpuGigaFlops = (1.0E-9 * flops) / (msecGPUElapsedTime / 1000.0f);
        cpuGigaFlops = (1.0E-9 * flops) / (msecCPUElaptedTime / 1000.0f);
    
        formatGPUFGlops = String.format("%.2f", gpuGigaFlops);
        formatCPUFGlops = String.format("%.2f", cpuGigaFlops);
    
        System.out.println("\tOutliers Kernel CPU Execution: " + formatCPUFGlops + " GFlops, Total time = " + (endSequential - startSequential) + " ms");
        System.out.println("\tOutliers Kernel GPU Execution: " + formatGPUFGlops + " GFlops, Total Time = " + (end - start) + " ms");
        System.out.println("\tOutliers Kernel Speedup: " + ((endSequential - startSequential) / (end - start)) + "x");
    
    
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
