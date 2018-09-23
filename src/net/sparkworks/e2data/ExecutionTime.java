package net.sparkworks.e2data;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;

public class ExecutionTime {
    
    public static <T> T printTime(Callable<T> task) {
        T call = null;
        try {
            Instant start = Instant.now();
            call = task.call();
            System.out.print(String.format("Execution time : %s of: ",
                    Duration.between(start, Instant.now()).toString()));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return call;
    }
    
}
