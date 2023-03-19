package org.apache.lucene.search;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

public class SearchWorkers {

    final private static QueryExecutor[] executors = {new QueryExecutor(), new QueryExecutor()};

    final private static Map<Object, QueryExecutor> register = new ConcurrentHashMap<>();
    final private static Map<Object, Integer> taskCounter = new ConcurrentHashMap<>();

    final private static Map<Object, List<Future>> missions = new ConcurrentHashMap<>();

    final private static BiFunction<Object, Integer, Integer> countUp = (k, v) -> {
        if (v == null)
            return 1;
        return v + 1;
    };

    final private static BiFunction<Object, Integer, Integer> countDown = (k, v) -> v - 1;

    final private static AtomicInteger counter = new AtomicInteger(0);

    private static Thread getTaskOwner(){
        return Thread.currentThread();
    }

    public static void execute(Runnable task) {
        Thread taskOwner = getTaskOwner();
        taskCounter.compute(taskOwner, countUp);
        QueryExecutor executor = register.compute(taskOwner, (k, v)->{
            if (v == null) return executors[counter.incrementAndGet() % executors.length];
            return v;
        });
        executor.pool.execute(() -> {
            task.run();
            taskCounter.compute(taskOwner, countDown);
            synchronized (taskOwner) {
                taskOwner.notifyAll();
            }
        });
    }

    public static void awaitForTasks() {
        Thread taskOwner = getTaskOwner();
        synchronized (taskOwner) {
            try {
                while (taskCounter.get(taskOwner) > 0) {
                    taskOwner.wait();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            taskCounter.remove(taskOwner);
            register.remove(taskOwner);
        }
    }

    public static <C> void submit(Callable<C> task) {
        Thread taskOwner = getTaskOwner();
        missions.compute(taskOwner, (k, v) -> {
            if (v == null) v = new ArrayList<>();
            v.add(register.get(taskOwner).pool.submit(task));
            return v;
        });
    }

    public static <C> List<C> awaitForTasksResult() {
        Thread taskOwner = getTaskOwner();
        List<C> result = new ArrayList<>();
        for (Future future : missions.get(taskOwner)) {
            try {
                result.add((C) future.get());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        missions.remove(taskOwner);
        return result;
    }

    private static class QueryExecutor{
        final ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);
    }
}
