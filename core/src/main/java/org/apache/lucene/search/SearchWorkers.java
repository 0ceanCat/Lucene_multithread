//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.lucene.search;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

public class SearchWorkers {
    public static final int THREADS = 3;
    private static final QueryExecutor[] executors = new QueryExecutor[]{new QueryExecutor(), new QueryExecutor()};
    private static final Map<Object, QueryExecutor> register = new ConcurrentHashMap();
    private static final Map<Object, Integer> taskCounter = new ConcurrentHashMap();
    private static final Map<Object, List<Future>> missions = new ConcurrentHashMap();
    private static final BiFunction<Object, Integer, Integer> countUp = (k, v) -> {
        return v == null ? 1 : v + 1;
    };
    private static final BiFunction<Object, Integer, Integer> countDown = (k, v) -> {
        return v - 1;
    };
    private static final AtomicInteger counter = new AtomicInteger(0);

    private SearchWorkers() {
    }

    private static Thread getTaskOwner() {
        return Thread.currentThread();
    }

    public static void execute(Runnable task) {
        Thread taskOwner = getTaskOwner();
        taskCounter.compute(taskOwner, countUp);
        QueryExecutor executor = (QueryExecutor)register.compute(taskOwner, (k, v) -> {
            return v == null ? executors[counter.incrementAndGet() % executors.length] : v;
        });
        executor.pool.execute(() -> {
            task.run();
            taskCounter.compute(taskOwner, countDown);
            synchronized(taskOwner) {
                taskOwner.notifyAll();
            }
        });
    }

    public static void awaitForTasks() {
        Thread taskOwner = getTaskOwner();
        synchronized(taskOwner) {
            try {
                while(taskCounter.get(taskOwner) > 0) {
                    taskOwner.wait();
                }
            } catch (InterruptedException var4) {
                var4.printStackTrace();
            }

            taskCounter.remove(taskOwner);
            register.remove(taskOwner);
        }
    }

    public static <C> void submit(Callable<C> task) {
        Thread taskOwner = getTaskOwner();
        missions.compute(taskOwner, (k, v) -> {
            if (v == null) {
                v = new ArrayList<>();
            }

            v.add((register.get(taskOwner)).pool.submit(task));
            return v;
        });
    }

    public static <T> List<T> awaitForTasksResult() {
        Thread taskOwner = getTaskOwner();
        List<T> result = new ArrayList<>();
        for (Future<?> future : missions.get(taskOwner)) {
            try {
                result.add((T) future.get());
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }
        }

        missions.remove(taskOwner);
        return result;
    }

    private static class QueryExecutor {
        final ThreadPoolExecutor pool = (ThreadPoolExecutor)Executors.newFixedThreadPool(THREADS);

        private QueryExecutor() {
        }
    }
}
