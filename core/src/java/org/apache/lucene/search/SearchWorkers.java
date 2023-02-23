package org.apache.lucene.search;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.BiFunction;

public class SearchWorkers {
    final private static ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(6);
            /*new ThreadPoolExecutor(6,
                    16,
                    60L,
                    TimeUnit.SECONDS,
                    new LinkedBlockingQueue<Runnable>());*/

    final private static Map<Object, Integer> taskCounter = new ConcurrentHashMap<>();

  //  final private static Map<Object, List<Future>> missions = new ConcurrentHashMap<>();

    final private static BiFunction<Object, Integer, Integer> countUp = (k, v) -> {
        if(v == null)
            return  1;
        return v + 1;
    };

    final private static BiFunction<Object, Integer, Integer> countDown = (k, v) -> v - 1;

    static void execute(Object taskOwner, Runnable task){
        /*taskCounter.compute(taskOwner, countUp);
        pool.execute(() -> {
            task.run();
            taskCounter.compute(taskOwner, countDown);
            synchronized (taskOwner){
                taskOwner.notifyAll();
            }
        });*/
        pool.execute(task);
    }

   /* static void awaitForTasks(Object taskOwner){
        synchronized(taskOwner){
            try {
                while (taskCounter.get(taskOwner) > 0){
                    taskOwner.wait();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            taskCounter.remove(taskOwner);
        }
    }

    static <C> void submit(Object taskOwner, Callable<C> task){
        missions.compute(taskOwner, (k, v)->{
            if(v == null) v = new ArrayList<>();
            v.add(pool.submit(task));
            return v;
        });
    }

    static <C> List<C> awaitForTasksResult(Object taskOwner){
        List<C> result = new ArrayList<>();
        for (Future future : missions.get(taskOwner)) {
            try {
                result.add((C) future.get());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        return result;
    }*/
}
