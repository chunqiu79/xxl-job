package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.model.XxlJobInfo;
import com.xxl.job.admin.core.model.XxlJobLog;
import com.xxl.job.admin.core.trigger.TriggerTypeEnum;
import com.xxl.job.admin.core.util.I18nUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * job monitor instance
 *
 * @author xuxueli 2015-9-1 18:05:56
 */
public class JobFailMonitorHelper {
	private static Logger logger = LoggerFactory.getLogger(JobFailMonitorHelper.class);
	
	private static JobFailMonitorHelper instance = new JobFailMonitorHelper();
	public static JobFailMonitorHelper getInstance(){
		return instance;
	}

	// ---------------------- monitor ----------------------

	private Thread monitorThread;
	private volatile boolean toStop = false;
	public void start() {
		// 监控线程
		monitorThread = new Thread(() -> {

            // monitor
            while (!toStop) {
                try {
					// 查询xxl_job_log中失败的(具体看sql)id,1次查询1000个
                    List<Long> failLogIds = XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().findFailJobLogIds(1000);
                    if (failLogIds != null && !failLogIds.isEmpty()) {
                        for (long failLogId: failLogIds) {
							// 通过id更新xxl_job_log的alarm_status设置成-1,其实就是相当于加锁
                            int lockRet = XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().updateAlarmStatus(failLogId, 0, -1);
                            if (lockRet < 1) {
                                continue;
                            }
                            // 通过id查询xxl_job_log的数据
                            XxlJobLog log = XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().load(failLogId);
                            // 通过xxl_job_log的job_id查询xxl_job_info的数据
                            XxlJobInfo info = XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().loadById(log.getJobId());

                            // 1、fail retry monitor
                            if (log.getExecutorFailRetryCount() > 0) {
                                // xxl_job_log的executor_fail_retry_count>0

                                JobTriggerPoolHelper.trigger(log.getJobId(), TriggerTypeEnum.RETRY, (log.getExecutorFailRetryCount()-1), log.getExecutorShardingParam(), log.getExecutorParam(), null);
                                String retryMsg = "<br><br><span style=\"color:#F39C12;\" > >>>>>>>>>>>"+ I18nUtil.getString("jobconf_trigger_type_retry") +"<<<<<<<<<<< </span><br>";
                                log.setTriggerMsg(log.getTriggerMsg() + retryMsg);
                                // 通过id更新xxl_job_log数据(这里主要更新trigger_msg)
                                XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().updateTriggerInfo(log);
                            }

                            // 2、fail alarm monitor
                            int newAlarmStatus = 0;		// 告警状态：0-默认、-1=锁定状态、1-无需告警、2-告警成功、3-告警失败
                            if (info != null && info.getAlarmEmail() != null && !info.getAlarmEmail().trim().isEmpty()) {
                                boolean alarmResult = XxlJobAdminConfig.getAdminConfig().getJobAlarmer().alarm(info, log);
                                newAlarmStatus = alarmResult ? 2 : 3;
                            } else {
                                newAlarmStatus = 1;
                            }
                            // 更新xxl_job_log的alarm_status
                            XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().updateAlarmStatus(failLogId, -1, newAlarmStatus);
                        }
                    }

                } catch (Exception e) {
                    if (!toStop) {
                        logger.error(">>>>>>>>>>> xxl-job, job fail monitor thread error:", e);
                    }
                }

                try {
                    // 睡10秒钟
                    TimeUnit.SECONDS.sleep(10);
                } catch (Exception e) {
                    if (!toStop) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }
            logger.info(">>>>>>>>>>> xxl-job, job fail monitor thread stop");
        });
        // 设置成守护线程
		monitorThread.setDaemon(true);
		monitorThread.setName("xxl-job, admin JobFailMonitorHelper");
		monitorThread.start();
	}

	public void toStop(){
		toStop = true;
		// interrupt and wait
		monitorThread.interrupt();
		try {
			monitorThread.join();
		} catch (InterruptedException e) {
			logger.error(e.getMessage(), e);
		}
	}

}
