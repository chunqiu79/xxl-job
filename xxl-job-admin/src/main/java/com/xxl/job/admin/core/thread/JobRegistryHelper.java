package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.model.XxlJobGroup;
import com.xxl.job.admin.core.model.XxlJobRegistry;
import com.xxl.job.core.biz.model.RegistryParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.enums.RegistryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * job registry instance
 * @author xuxueli 2016-10-02 19:10:24
 */
public class JobRegistryHelper {
	private static Logger logger = LoggerFactory.getLogger(JobRegistryHelper.class);

	private static JobRegistryHelper instance = new JobRegistryHelper();
	public static JobRegistryHelper getInstance(){
		return instance;
	}

	private ThreadPoolExecutor registryOrRemoveThreadPool = null;
	private Thread registryMonitorThread;
	private volatile boolean toStop = false;

	public void start() {
		// 注册|注销线程池
		// 核心线程-2, 最大核心线程数-10
		// 存活时间-30, 存活时间单位-秒
		// 阻塞队列大小-2000, 线程名字前缀-"xxl-job, admin JobRegistryMonitorHelper-registryOrRemoveThreadPool-"
		// 拒绝策略-使用当前线程执行&打印warning日志
		registryOrRemoveThreadPool = new ThreadPoolExecutor(
			2,
			10,
			30L,
			TimeUnit.SECONDS,
			new LinkedBlockingQueue<Runnable>(2000),
            r -> new Thread(r, "xxl-job, admin JobRegistryMonitorHelper-registryOrRemoveThreadPool-" + r.hashCode()),
            (r, executor) -> {
                r.run();
                logger.warn(">>>>>>>>>>> xxl-job, registry or remove too fast, match threadpool rejected handler(run now).");
            }
		);

		// 注册的监控线程
		registryMonitorThread = new Thread(() -> {
            while (!toStop) {
                try {
					// 从xxl_job_group表中读取(addressType=0的,即自动注册的)数据
                    List<XxlJobGroup> groupList = XxlJobAdminConfig.getAdminConfig().getXxlJobGroupDao().findByAddressType(0);
                    if (groupList != null && !groupList.isEmpty()) {
						// 存在自动注册的执行器数据

						// 从xxl_job_registry表中读取(updateTime<当前时间-90秒) 的 id
                        List<Integer> ids = XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().findDead(RegistryConfig.DEAD_TIMEOUT, new Date());
                        if (ids!=null && !ids.isEmpty()) {
							// 存在(updateTime<当前时间-90秒)的数据，则认为已经死亡，需要删除
							// TODO: 2024/8/7 为啥不直接删除的时候判断？
                            XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().removeDead(ids);
                        }

						// 存储所有的registry_group="EXECUTOR"的数据的registry-value
						// key: registryKey		value: List<registry-value>
                        HashMap<String, List<String>> appAddressMap = new HashMap<>();
						// 从xxl_job_registry表中读取(updateTime>当前时间-90秒) 的 数据
                        List<XxlJobRegistry> list = XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().findAll(RegistryConfig.DEAD_TIMEOUT, new Date());
                        if (list != null) {
							// 存在正常的数据
                            for (XxlJobRegistry item: list) {
                                if (RegistryConfig.RegistType.EXECUTOR.name().equals(item.getRegistryGroup())) {
									// registry_group="EXECUTOR"
                                    String appname = item.getRegistryKey();

									List<String> registryList = appAddressMap.computeIfAbsent(appname,
										k -> new ArrayList<>());

                                    if (!registryList.contains(item.getRegistryValue())) {
                                        registryList.add(item.getRegistryValue());
                                    }

                                }
                            }
                        }

                        // 循环所有自动注册的数据
                        for (XxlJobGroup group : groupList) {
                            List<String> registryList = appAddressMap.get(group.getAppname());

							// 将registryList排序，然后通过","拼接成addressList
							// 其实就是将xxl_job_registry表中相同的registry_key分组,然后将registry_value排序
							// 然后使用","拼接,赋值到xxl_job_group的address_list
							// 这里将两个表关联起来的xxl_job_group的appname==xxl_job_registry的registry_key
							if (registryList != null && !registryList.isEmpty()) {
								String addressListStr = registryList.stream()
									.sorted()
									.collect(Collectors.joining(","));
								group.setAddressList(addressListStr);
							} else {
								group.setAddressList(null);
							}
                            group.setUpdateTime(new Date());

							// 更新xxl_job_group表
                            XxlJobAdminConfig.getAdminConfig().getXxlJobGroupDao().update(group);
                        }
                    }
                } catch (Exception e) {
                    if (!toStop) {
                        logger.error(">>>>>>>>>>> xxl-job, job registry monitor thread error:", e);
                    }
                }
                try {
					// 睡30秒
                    TimeUnit.SECONDS.sleep(RegistryConfig.BEAT_TIMEOUT);
                } catch (InterruptedException e) {
                    if (!toStop) {
                        logger.error(">>>>>>>>>>> xxl-job, job registry monitor thread error:", e);
                    }
                }
            }
            logger.info(">>>>>>>>>>> xxl-job, job registry monitor thread stop");
        });
		// 设置为守护线程
		registryMonitorThread.setDaemon(true);
		registryMonitorThread.setName("xxl-job, admin JobRegistryMonitorHelper-registryMonitorThread");
		registryMonitorThread.start();
	}

	public void toStop(){
		toStop = true;

		// stop registryOrRemoveThreadPool
		registryOrRemoveThreadPool.shutdownNow();

		// stop monitor (interrupt and wait)
		registryMonitorThread.interrupt();
		try {
			registryMonitorThread.join();
		} catch (InterruptedException e) {
			logger.error(e.getMessage(), e);
		}
	}


	// ---------------------- helper ----------------------

	public ReturnT<String> registry(RegistryParam registryParam) {

		// valid
		if (!StringUtils.hasText(registryParam.getRegistryGroup())
				|| !StringUtils.hasText(registryParam.getRegistryKey())
				|| !StringUtils.hasText(registryParam.getRegistryValue())) {
			return new ReturnT<>(ReturnT.FAIL_CODE, "Illegal Argument.");
		}

		// 使用线程池进行执行
		registryOrRemoveThreadPool.execute(() -> {
			// 修改 xxl_job_registry表 中客户端注册信息的 修改时间
            int ret = XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().registryUpdate(registryParam.getRegistryGroup(), registryParam.getRegistryKey(), registryParam.getRegistryValue(), new Date());
            if (ret < 1) {
				// ret < 1, 表明 xxl_job_registry表 中没有对应的客户端注册信息
				// 在 xxl_job_registry表 中创建对应的注册信息
                XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().registrySave(registryParam.getRegistryGroup(), registryParam.getRegistryKey(), registryParam.getRegistryValue(), new Date());
            }
        });

		return ReturnT.SUCCESS;
	}

	public ReturnT<String> registryRemove(RegistryParam registryParam) {

		// valid
		if (!StringUtils.hasText(registryParam.getRegistryGroup())
				|| !StringUtils.hasText(registryParam.getRegistryKey())
				|| !StringUtils.hasText(registryParam.getRegistryValue())) {
			return new ReturnT<>(ReturnT.FAIL_CODE, "Illegal Argument.");
		}

		// async execute
		registryOrRemoveThreadPool.execute(() -> {
			// 从 xxl_job_registry表 中删除客户端注册信息
            XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().registryDelete(registryParam.getRegistryGroup(), registryParam.getRegistryKey(), registryParam.getRegistryValue());
        });

		return ReturnT.SUCCESS;
	}


}
