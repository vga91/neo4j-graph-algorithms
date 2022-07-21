package org.neo4j.graphalgo.core.utils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;


//
//
//
// TODO - create Runable directly instead of NamedThreadFactory.daemon("algo"),
//
// TODO -- > probabilmente posso estendere il DefaultThreadFactory cambiando solo il threadNamePrefix 
//
//
public class NamedThreadFactory implements ThreadFactory {
    /** @deprecated */
    @Deprecated
    public static final NamedThreadFactory.Monitor NO_OP_MONITOR = new NamedThreadFactory.Monitor() {
        public void threadCreated(String threadNamePrefix) {
        }

        public void threadFinished(String threadNamePrefix) {
        }
    };
    private static final int DEFAULT_THREAD_PRIORITY = 5;
    private final ThreadGroup group;
    private final AtomicInteger threadCounter;
    private String threadNamePrefix;
    private final int priority;
    private final boolean daemon;
    private final NamedThreadFactory.Monitor monitor;

    /** @deprecated */
    @Deprecated
    public NamedThreadFactory(String threadNamePrefix) {
        this(threadNamePrefix, 5);
    }

    /** @deprecated */
    @Deprecated
    public NamedThreadFactory(String threadNamePrefix, int priority) {
        this(threadNamePrefix, priority, NO_OP_MONITOR);
    }

    /** @deprecated */
    @Deprecated
    public NamedThreadFactory(String threadNamePrefix, NamedThreadFactory.Monitor monitor) {
        this(threadNamePrefix, 5, monitor);
    }

    /** @deprecated */
    @Deprecated
    public NamedThreadFactory(String threadNamePrefix, int priority, NamedThreadFactory.Monitor monitor) {
        this(threadNamePrefix, priority, monitor, false);
    }

    /** @deprecated */
    @Deprecated
    public NamedThreadFactory(String threadNamePrefix, int priority, boolean daemon) {
        this(threadNamePrefix, priority, NO_OP_MONITOR, daemon);
    }

    /** @deprecated */
    @Deprecated
    public NamedThreadFactory(String threadNamePrefix, boolean daemon) {
        this(threadNamePrefix, 5, NO_OP_MONITOR, daemon);
    }

    /** @deprecated */
    @Deprecated
    public NamedThreadFactory(String threadNamePrefix, int priority, NamedThreadFactory.Monitor monitor, boolean daemon) {
        this.threadCounter = new AtomicInteger(1);
        this.threadNamePrefix = threadNamePrefix;
        SecurityManager securityManager = System.getSecurityManager();
        this.group = securityManager != null ? securityManager.getThreadGroup() : Thread.currentThread().getThreadGroup();
        this.priority = priority;
        this.daemon = daemon;
        this.monitor = monitor;
    }

    // todo - credo non serva tutto sto casino...
    // todo - provare a scopiazzare qualcosa da apoc
    public Thread newThread(Runnable runnable) {
        int id = this.threadCounter.getAndIncrement();
        Thread result = new Thread(this.group, runnable, this.threadNamePrefix + "-" + id) {
            public void run() {
                try {
                    super.run();
                } finally {
                    NamedThreadFactory.this.monitor.threadFinished(NamedThreadFactory.this.threadNamePrefix);
                }

            }
        };
        result.setDaemon(this.daemon);
        result.setPriority(this.priority);
        this.monitor.threadCreated(this.threadNamePrefix);
        return result;
    }

    /** @deprecated */
    @Deprecated
    public static NamedThreadFactory named(String threadNamePrefix) {
        return new NamedThreadFactory(threadNamePrefix);
    }

    /** @deprecated */
    @Deprecated
    public static NamedThreadFactory named(String threadNamePrefix, int priority) {
        return new NamedThreadFactory(threadNamePrefix, priority);
    }

    /** @deprecated */
    @Deprecated
    public static NamedThreadFactory daemon(String threadNamePrefix) {
        return daemon(threadNamePrefix, NO_OP_MONITOR);
    }

    /** @deprecated */
    @Deprecated
    public static NamedThreadFactory daemon(String threadNamePrefix, NamedThreadFactory.Monitor monitor) {
        return new NamedThreadFactory(threadNamePrefix, 5, monitor, true);
    }

    /** @deprecated */
    @Deprecated
    public interface Monitor {
        /** @deprecated */
        @Deprecated
        void threadCreated(String var1);

        /** @deprecated */
        @Deprecated
        void threadFinished(String var1);
    }
}