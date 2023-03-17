package org.apache.lucene.search;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

public class SearchWorkers {
    final private static QueryExecutor[] executors = {new QueryExecutor(), new QueryExecutor()};

    final private static Map<Thread, Integer> register = new ConcurrentHashMap<>();
    final private static Map<Thread, Integer> taskCounter = new ConcurrentHashMap<>();

    final private static Map<Thread, List<Future>> missions = new ConcurrentHashMap<>();

    final private static AtomicInteger registCounter = new AtomicInteger(0);
    final private static BiFunction<Thread, Integer, Integer> countUp = (k, v) -> {
        if (v == null)
            return 1;
        return v + 1;
    };

    final private static BiFunction<Thread, Integer, Integer> countDown = (k, v) -> v - 1;
    final private static BiFunction<Thread, Integer, Integer> regist = (k, v) -> {
        if (v == null) return registCounter.incrementAndGet() % executors.length;
        return v;
    };

    private static Thread getTaskOwner() {
        return Thread.currentThread();
    }

    static void execute(Runnable task) {
        Thread taskOwner = getTaskOwner();
        taskCounter.compute(taskOwner, countUp);
        Integer indx = register.compute(taskOwner, regist);
        executors[indx].pool.execute(() -> {
            task.run();
            taskCounter.compute(taskOwner, countDown);
            synchronized (taskOwner) {
                taskOwner.notifyAll();
            }
        });
    }

    static void awaitForTasks() {
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

    static <C> void submit(Callable<C> task) {
        Thread taskOwner = getTaskOwner();
        Integer indx = register.compute(taskOwner, regist);
        missions.compute(taskOwner, (k, v) -> {
            if (v == null) v = new ArrayList<>();
            v.add(executors[indx].pool.submit(task));
            return v;
        });
    }

    static <C> List<C> awaitForTasksResult() {
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
        register.remove(taskOwner);
        return result;
    }

    private static class QueryExecutor {
        final ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);
    }
}
