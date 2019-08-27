package net.sparkworks.e2data;

public class ExecutionTime {
    
    public static void printTime(Runnable task, int repeats) {
        try {
            long sum = 0;
            for (int i = 0; i < repeats; i++) {
                long start = System.nanoTime();
                task.run();
                long dur = System.nanoTime() - start;
                sum += dur;
            }
            
            System.out.print("Execution time: " + (sum / repeats) + " ns of: ");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}
