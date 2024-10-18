package com.xxl.job.core.handler.impl;

import com.xxl.job.core.handler.IJobHandler;

import java.lang.reflect.Method;

/**
 * 使用@XxlJob 方法级别的
 */
public class MethodJobHandler extends IJobHandler {

    private final Object target;
    private final Method method;
    private final Method initMethod;
    private final Method destroyMethod;

    public MethodJobHandler(Object target, Method method, Method initMethod, Method destroyMethod) {
        this.target = target;
        this.method = method;
        this.initMethod = initMethod;
        this.destroyMethod = destroyMethod;
    }

    @Override
    public void execute() throws Exception {
        // 其实就是 spring的代理类 执行标记有 @XxlJob 的方法 (只是需要当前执行的方法)
        // for example: com.xxl.job.executor.service.jobhandler.SampleXxlJob.demoJobHandler
        Class<?>[] paramTypes = method.getParameterTypes();
        if (paramTypes.length > 0) {
            method.invoke(target, new Object[paramTypes.length]);
        } else {
            method.invoke(target);
        }
    }

    @Override
    public void init() throws Exception {
        if (initMethod != null) {
            // @XxlJob 注解上指定的 init方法
            initMethod.invoke(target);
        }
    }

    @Override
    public void destroy() throws Exception {
        if (destroyMethod != null) {
            // @XxlJob 注解上指定的 destroy方法
            destroyMethod.invoke(target);
        }
    }

    @Override
    public String toString() {
        return super.toString() + "[" + target.getClass() + "#" + method.getName() + "]";
    }

}
