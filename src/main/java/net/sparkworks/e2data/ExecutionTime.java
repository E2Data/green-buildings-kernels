package net.sparkworks.e2data;

import java.time.Duration;
import java.time.Instant;

public class ExecutionTime {
    
    public static void printTime(Runnable task) {
        try {
            Instant start = Instant.now();
            task.run();
            System.out.print(String.format("Execution time : %s of: ",
                    Duration.between(start, Instant.now()).toString()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}
