package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.trigger.TriggerTypeEnum;
import com.xxl.job.admin.core.trigger.XxlJobTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * job trigger thread pool helper
 *
 * @author xuxueli 2018-07-03 21:08:07
 */
public class JobTriggerPoolHelper {
    private static Logger logger = LoggerFactory.getLogger(JobTriggerPoolHelper.class);


    // ---------------------- trigger pool ----------------------

    // fast/slow thread pool
    private ThreadPoolExecutor fastTriggerPool = null;
    private ThreadPoolExecutor slowTriggerPool = null;

    /**
     * 创建2个线程池
     */
    public void start() {
        // 快触发器线程池
        // 核心线程-10, 最大线程-Max(xxl.job.triggerpool.fast.max, 200)
        // 存活时间-60, 存活时间单位-秒
        // 阻塞队列大小-1000, 线程名字前缀-"xxl-job, admin JobTriggerPoolHelper-fastTriggerPool-"
        // 拒绝策略-默认拒绝策略,直接丢弃
        fastTriggerPool = new ThreadPoolExecutor(
            10,
            XxlJobAdminConfig.getAdminConfig().getTriggerPoolFastMax(),
            60L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(1000),
            r -> new Thread(r, "xxl-job, admin JobTriggerPoolHelper-fastTriggerPool-" + r.hashCode())
        );

        // 慢触发器线程池
        // 核心线程-10, 最大线程-Max(xxl.job.triggerpool.slow.max, 200)
        // 存活时间-60, 存活时间单位-秒
        // 阻塞队列大小-2000, 线程名字前缀-"xxl-job, admin JobTriggerPoolHelper-slowTriggerPool-"
        // 拒绝策略-默认拒绝策略,直接丢弃
        slowTriggerPool = new ThreadPoolExecutor(
            10,
            XxlJobAdminConfig.getAdminConfig().getTriggerPoolSlowMax(),
            60L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(2000),
            r -> new Thread(r, "xxl-job, admin JobTriggerPoolHelper-slowTriggerPool-" + r.hashCode())
        );
    }


    public void stop() {
        fastTriggerPool.shutdownNow();
        slowTriggerPool.shutdownNow();
        logger.info(">>>>>>>>> xxl-job trigger thread pool shutdown success.");
    }


    private volatile long minTim = System.currentTimeMillis() / 60_000;
    /**
     * jobTimeoutCountMap中的jobId对应的计数如果超过10次,那么就是使用 slowTriggerPool线程池 来执行任务
      */
    private volatile ConcurrentMap<Integer, AtomicInteger> jobTimeoutCountMap = new ConcurrentHashMap<>();


    /**
     * add trigger
     */
    public void addTrigger(final int jobId,
                           final TriggerTypeEnum triggerType,
                           final int failRetryCount,
                           final String executorShardingParam,
                           final String executorParam,
                           final String addressList) {

        // 默认使用 fastTriggerPool线程池,但是如果(1分钟之内)用时大于500毫秒10次以上就使用 slowTriggerPool线程池
        ThreadPoolExecutor triggerPool = fastTriggerPool;
        AtomicInteger jobTimeoutCount = jobTimeoutCountMap.get(jobId);
        if (jobTimeoutCount != null && jobTimeoutCount.get() > 10) {
            triggerPool = slowTriggerPool;
        }

        // trigger
        triggerPool.execute(() -> {
            long start = System.currentTimeMillis();
            try {
                // 执行
                XxlJobTrigger.trigger(jobId, triggerType, failRetryCount, executorShardingParam, executorParam, addressList);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            } finally {

                // check timeout-count-map
                long minTimNow = System.currentTimeMillis() / 60000;
                // 这里的目的就是为了判断是不是同1分钟
                if (minTim != minTimNow) {
                    // 不是同1分钟内，那么就刷新minTim,将jobTimeoutCountMap清空
                    minTim = minTimNow;
                    jobTimeoutCountMap.clear();
                }
                // 用来统计作业执行时间
                long cost = System.currentTimeMillis() - start;
                if (cost > 500) {
                    // 如果执行时间大于500毫秒,则添加到jobTimeoutCountMap,计数+1
                    // jobTimeoutCountMap中的jobId对应的计数如果超过10次(1分钟之内),那么就是使用 slowTriggerPool线程池 来执行任务
                    AtomicInteger timeoutCount = jobTimeoutCountMap.putIfAbsent(jobId, new AtomicInteger(1));
                    if (timeoutCount != null) {
                        timeoutCount.incrementAndGet();
                    }
                }

            }

        });
    }


    // ---------------------- helper ----------------------

    private static JobTriggerPoolHelper helper = new JobTriggerPoolHelper();

    public static void toStart() {
        helper.start();
    }

    public static void toStop() {
        helper.stop();
    }

    /**
     * @param jobId
     * @param triggerType
     * @param failRetryCount        >=0: use this param
     *                              <0: use param from job info config
     * @param executorShardingParam
     * @param executorParam         null: use job param
     *                              not null: cover job param
     */
    public static void trigger(int jobId, TriggerTypeEnum triggerType, int failRetryCount, String executorShardingParam, String executorParam, String addressList) {
        helper.addTrigger(jobId, triggerType, failRetryCount, executorShardingParam, executorParam, addressList);
    }

}
