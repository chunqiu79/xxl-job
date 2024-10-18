package com.xxl.job.admin.core.scheduler;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.thread.*;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.client.ExecutorBizClient;
import com.xxl.job.core.enums.ExecutorBlockStrategyEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author xuxueli 2018-10-28 00:18:17
 */

public class XxlJobScheduler {
    private static final Logger logger = LoggerFactory.getLogger(XxlJobScheduler.class);

    /**
     * 调度初始化核心逻辑
     */
    public void init() throws Exception {
        // init i18n
        // 国际化设置
        initI18n();

        // admin trigger pool start
        // 创建2个触发器线程池
        // 1个快 1个慢
        JobTriggerPoolHelper.toStart();

        // admin registry monitor run
        // 创建1个注册|注销线程池,创建1个注册的监控线程
        JobRegistryHelper.getInstance().start();

        // admin fail-monitor run
        // 创建1个失败的监控线程
        JobFailMonitorHelper.getInstance().start();

        // admin lose-monitor run ( depend on JobTriggerPoolHelper )
        // 创建1个线程池,创建1个线程
        JobCompleteHelper.getInstance().start();

        // admin log report start
        // 创建1个运行报表的线程
        JobLogReportHelper.getInstance().start();

        // start-schedule  ( depend on JobTriggerPoolHelper )
        // 创建2个线程
        JobScheduleHelper.getInstance().start();

        logger.info(">>>>>>>>> init xxl-job admin success.");
    }

    /**
     * 调度销毁核心逻辑
     */
    public void destroy() throws Exception {

        // stop-schedule
        JobScheduleHelper.getInstance().toStop();

        // admin log report stop
        JobLogReportHelper.getInstance().toStop();

        // admin lose-monitor stop
        JobCompleteHelper.getInstance().toStop();

        // admin fail-monitor stop
        JobFailMonitorHelper.getInstance().toStop();

        // admin registry stop
        JobRegistryHelper.getInstance().toStop();

        // admin trigger pool stop
        JobTriggerPoolHelper.toStop();

    }

    // ---------------------- I18n ----------------------

    private void initI18n() {
        for (ExecutorBlockStrategyEnum item : ExecutorBlockStrategyEnum.values()) {
            item.setTitle(I18nUtil.getString("jobconf_block_".concat(item.name())));
        }
    }

    // ---------------------- executor-client ----------------------
    /**
     * 缓存,用来存储 执行器地址 和 执行器的相关信息 映射关系
     * key-执行器地址    value-执行器的相关信息
     */
    private static ConcurrentMap<String, ExecutorBiz> executorBizRepository = new ConcurrentHashMap<>();

    public static ExecutorBiz getExecutorBiz(String address) {
        if (address == null || address.trim().isEmpty()) {
            return null;
        }
        address = address.trim();
        ExecutorBiz executorBiz = executorBizRepository.get(address);
        if (executorBiz != null) {
            return executorBiz;
        }
        // 设置缓存(这里创建的是ExecutorBizClient)
        executorBiz = new ExecutorBizClient(address, XxlJobAdminConfig.getAdminConfig().getAccessToken());
        executorBizRepository.put(address, executorBiz);
        return executorBiz;
    }

}
