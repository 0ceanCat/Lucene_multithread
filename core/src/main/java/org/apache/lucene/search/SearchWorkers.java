package org.apache.lucene.search;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.BiFunction;

public class SearchWorkers {
    final private static ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        /*new ThreadPoolExecutor(6,
            16,
            60L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>());*/

    final private static Map<Object, Integer> taskCounter = new ConcurrentHashMap<>();

    final private static Map<Object, List<Future>> missions = new ConcurrentHashMap<>();

    final private static BiFunction<Object, Integer, Integer> countUp = (k, v) -> {
        if (v == null)
            return 1;
        return v + 1;
    };

    final private static BiFunction<Object, Integer, Integer> countDown = (k, v) -> v - 1;

    private static Thread getTaskOwner(){
        return Thread.currentThread();
    }

    static void execute(Runnable task) {
        Thread taskOwner = getTaskOwner();
        taskCounter.compute(taskOwner, countUp);
        pool.execute(() -> {
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
        }
    }

    static <C> void submit(Callable<C> task) {
        missions.compute(Thread.currentThread(), (k, v) -> {
            if (v == null) v = new ArrayList<>();
            v.add(pool.submit(task));
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
        return result;
    }
}
