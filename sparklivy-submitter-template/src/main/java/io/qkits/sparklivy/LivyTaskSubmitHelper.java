package io.qkits.sparklivy;

import io.qkits.sparklivy.config.SparkJobConfig;
import io.qkits.sparklivy.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@Slf4j
public class LivyTaskSubmitHelper {

    private ConcurrentMap<Long, Integer> taskAppIdMap = new ConcurrentHashMap<>();
    private AtomicInteger curConcurrentTaskNum = new AtomicInteger(0);
    private static final String REQUEST_BY_HEADER = "X-Requested-By";
    private RestTemplate restTemplate = new RestTemplate();
    // queue for pub or sub
    public static final int DEFAULT_QUEUE_SIZE = 20000;
    private static final int SLEEP_TIME = 300;

    @Value("${livy.task.max.concurrent.count:20}")
    private int maxConcurrentTaskCount;
    @Value("${livy.task.submit.interval.second:3}")
    private int batchIntervalSecond;

    /**
     * 初始化Livy 访问请求的RequestBody字段
     *
     * @param config
     * @return
     */
    private Map<String, Object> buildLivyArgs(SparkJobConfig config) {
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("file", config.getFile());
        List<String> args = new ArrayList<>();
        args.add(config.getRunSql());
        requestBody.put("className", config.getClassName());
        requestBody.put("args", args);
        return requestBody;
    }

    /**
     * create spark batch job through livy post:/batches
     *
     * @param config
     * @return batch api response
     */
    public Map<String, Object> createBatch(SparkJobConfig config) {
        Map<String, Object> result = new HashMap<>();
        try {
            HttpHeaders headers = buildLivyHeader();
            HttpEntity<String> springEntity = new HttpEntity<>(JsonUtil.toJsonWithFormat(buildLivyArgs(config)), headers);
            result = restTemplate.postForObject(config.getLivyUri(), springEntity, Map.class);
            log.info(result.toString());
        } catch (HttpClientErrorException e) {
            log.error("Post to livy ERROR. \n {} {}",
                    e.getMessage(),
                    e.getResponseBodyAsString());
        } catch (Exception e) {
            log.error("Post to livy ERROR. {}", e.getMessage());
        }
        return result;
    }

    /**
     * build default livy header
     *
     * @return
     */
    private HttpHeaders buildLivyHeader() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set(REQUEST_BY_HEADER, "admin");
        return headers;
    }

    /**
     * get batch status
     *
     * @param id
     * @param config
     * @return
     */
    public Map<String, Object> getBatchStatus(String id, SparkJobConfig config) {
        String getBatchURI = config.getLivyUri() + "/" + id;
        log.info("request_uri =", getBatchURI);
        return restTemplate.getForObject(getBatchURI, Map.class);

    }
}
