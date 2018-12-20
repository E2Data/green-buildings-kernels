package net.sparkworks.e2data;

import java.time.Duration;
import java.time.Instant;

public class ExecutionTime {
    
    public static void printTime(Runnable task) {
        try {
            
            long start = System.nanoTime();
            task.run();
            System.out.print("Execution time: " + (System.nanoTime() - start) + " ns of: ");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}
