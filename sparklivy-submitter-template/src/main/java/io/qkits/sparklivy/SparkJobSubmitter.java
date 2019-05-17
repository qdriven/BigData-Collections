package io.qkits.sparklivy;

import io.qkits.sparklivy.config.SparkJobConfig;
import io.qkits.sparklivy.context.SparkJobDetail;
import io.qkits.sparklivy.exception.SparkJobNotCompletedException;
import io.qkits.sparklivy.exception.SparkJobSubmitException;
import io.qkits.sparklivy.livy.LivySessionStates;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;


@Slf4j
@Service
public class SparkJobSubmitter {

    private static final int SLEEP_TIME = 1500;
    private static Integer MAX_RETRY_COUNT = 50;
    @Autowired
    private LivyTaskSubmitHelper livyTaskSubmitHelper;
    //todo: queue size
    private BlockingQueue<SparkJobDetail> waitingCompletedQueue = new LinkedBlockingQueue<>();
    private BlockingQueue<SparkJobDetail> waitingSubmitQueue = new LinkedBlockingQueue<>();
    public static final int DEFAULT_QUEUE_SIZE = 20000;

    public SparkJobSubmitter() {
        startWorker();
    }

    public void startWorker() {
        waitingCompletedQueue = new LinkedBlockingQueue<>(DEFAULT_QUEUE_SIZE);
        ExecutorService es4Submit = Executors.newSingleThreadExecutor();
        ExecutorService es4State = Executors.newSingleThreadExecutor();
        SubmitTaskInner submitTaskInner = new SubmitTaskInner(es4Submit);
        es4Submit.execute(submitTaskInner);
        StateTaskInner stateTaskInner = new StateTaskInner(es4Submit);
        es4State.submit(stateTaskInner);
    }

    public BlockingQueue<SparkJobDetail> getWaitingCompletedQueue() {
        return waitingCompletedQueue;
    }

    public void setWaitingCompletedQueue(BlockingQueue<SparkJobDetail> waitingCompletedQueue) {
        this.waitingCompletedQueue = waitingCompletedQueue;
    }

    public BlockingQueue<SparkJobDetail> getWaitingSubmitQueue() {
        return waitingSubmitQueue;
    }

    public void setWaitingSubmitQueue(BlockingQueue<SparkJobDetail> waitingSubmitQueue) {
        this.waitingSubmitQueue = waitingSubmitQueue;
    }

    class SubmitTaskInner implements Runnable {
        private ExecutorService es;

        public SubmitTaskInner(ExecutorService es) {
            this.es = es;
        }

        public void run() {
            while (true) {
                try {
                    //todo: what if submit always failed
                    SparkJobDetail sparkJobDetail = waitingSubmitQueue.take();
                    if (sparkJobDetail != null) {
                        Map<String, Object> result = livyTaskSubmitHelper.createBatch(sparkJobDetail.getConfig());
                        if (result.get("id") == null) waitingSubmitQueue.put(sparkJobDetail);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }
    }

    class StateTaskInner implements Runnable {
        private ExecutorService es;

        public StateTaskInner(ExecutorService es) {
            this.es = es;
        }

        public void run() {
            while (true) {
                try {
                    //todo: what if submit always failed
                    SparkJobDetail sparkJobDetail = waitingCompletedQueue.take();
                    Map<String, Object> batchStatus = livyTaskSubmitHelper.getBatchStatus(sparkJobDetail.getSparkJobId().toString(),
                            sparkJobDetail.getConfig());
                    System.out.println(batchStatus.get("state").toString());
                    if (LivySessionStates.isCompleted(batchStatus.get("state").toString())) {
                        //todo: doing something send message
                        System.out.println(batchStatus.get("id") + "job is completed");
                    } else {
                        waitingCompletedQueue.put(sparkJobDetail);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * submit spark job and want to get result
     */
    public Map<String, Object> submitAndGetJobResult(SparkJobConfig config) {

        Map<String, Object> result = livyTaskSubmitHelper.createBatch(config);
        if (result.get("id") != null) {
            int retryCount = 0;
            while (retryCount < MAX_RETRY_COUNT) {
                Map<String, Object> batchStatus = livyTaskSubmitHelper.getBatchStatus(result.get("id").toString()
                        , config);

                if (LivySessionStates.isCompleted(batchStatus.get("state").toString())) {
                    return batchStatus;
                }
                try {
                    Thread.sleep(SLEEP_TIME);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                retryCount++;
            }
        } else {
            throw new SparkJobSubmitException("creat spark batch failed!");
        }

        try {
            waitingCompletedQueue.put(
                    SparkJobDetail.builder()
                            .sparkJobId(Integer.parseInt(result.get("id").toString()))
                            .config(config)
                            .build()
            );
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        throw new SparkJobNotCompletedException("spark batch job is created, but not finished yet, " +
                "the result will be sent to you several minutes later");
    }

    public void submitJob(SparkJobConfig config) {
        Map<String, Object> result = livyTaskSubmitHelper.createBatch(config);
        if (result.get("id") != null) {
            SparkJobDetail sparkJobDetail = SparkJobDetail.builder().build();
            sparkJobDetail.setConfig(config);
            sparkJobDetail.setSparkJobId(Integer.parseInt(result.get("id").toString()));
            try {
                log.info("put job in to wait completed queue ", sparkJobDetail);
                waitingCompletedQueue.put(sparkJobDetail);
            } catch (InterruptedException e) {
                //todo: handle this exception
                e.printStackTrace();
            }
        } else {
            try {
                log.info("put job in to wait submit completed queue ", config);
                waitingSubmitQueue.put(SparkJobDetail.builder().config(config).build());
            } catch (InterruptedException e) {
                //todo: handle this exception
                e.printStackTrace();
            }
        }
    }

    //todo: use @Autowired instead
    public void setLivyTaskSubmitHelper(LivyTaskSubmitHelper livyTaskSubmitHelper) {
        this.livyTaskSubmitHelper = livyTaskSubmitHelper;
    }
}
