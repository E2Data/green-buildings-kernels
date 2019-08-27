package net.sparkworks.e2data;

public class ExecutionTime {
    
    public static void printTime(Runnable task) {
        try {
            long sum = 0;
            for (int i = 0; i < 10; i++) {
                long start = System.nanoTime();
                task.run();
                long dur = System.nanoTime() - start;
                sum += dur;
            }
            
            System.out.print("Execution time: " + (sum / 10) + " ns of: ");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}
