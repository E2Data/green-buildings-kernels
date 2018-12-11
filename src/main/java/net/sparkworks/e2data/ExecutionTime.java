package net.sparkworks.e2data;

import java.time.Duration;
import java.time.Instant;

public class ExecutionTime {
    
    public static void printTime(Runnable task) {
        try {
            Instant start = Instant.now();
            task.run();
            System.out.print(String.format("Execution time: %d ms of: ",
                    Duration.between(start, Instant.now()).toMillis()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}
