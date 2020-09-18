package com.pactera.bigdata.elasticsearch.sinker.synch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * @author liusinan
 * @version 1.0.0
 * @ClassName ElasticsearchSink.java
 * @Description TODO
 * @createTime 2020年08月08日 15:19:00
 */
public class ElasticsearchSink implements ElasticsearchImport {

    private static final Log LOGGER = LogFactory.getLog(ElasticsearchSink.class);

    //ES集群的所有host信息（192.168.1.1:9300）
    private static List<String> esHostTcpList = new ArrayList<>();
    private static String clusterName;

    private static ByteSizeValue byteSizeValue;

    private static ElasticsearchSink elasticsearchSink = null;
//    private static List<ElasticsearchSink> elasticsearchSinkList = new ArrayList<>();

    public ElasticsearchSink(String clusterName, List<String> esHostTcpList) {
        this.clusterName = clusterName;
        this.esHostTcpList = esHostTcpList;
    }

    public static ElasticsearchSink getInstance(String clusterName, List<String> esHostTcpList) {
////        if (elasticsearchSinkList.containsKey(clusterName) && elasticsearchSinkList.containsValue(esHostTcpList)) {
////            synchronized (ElasticsearchSink.class) {
////                elasticsearchSink = new ElasticsearchSink(clusterName, esHostTcpList);
////            }
////        }
//
//        if (elasticsearchSink != null) {
//            for (ElasticsearchSink elasticsearchSink : elasticsearchSinkList) {
//                if (elasticsearchSink.clusterName == clusterName && elasticsearchSink.esHostTcpList.equals(esHostTcpList)) {
//                    return elasticsearchSink;
//                } else {
//                    synchronized (ElasticsearchSink.class) {
//                        elasticsearchSink.elasticsearchSink = new ElasticsearchSink(clusterName, esHostTcpList);
//                        elasticsearchSinkList.add(elasticsearchSink.elasticsearchSink);
//                    }
//                }
//            }
////            if (elasticsearchSinkList(clusterName) && elasticsearchSinkList.get(clusterName).equals(esHostTcpList)) {
////                return elasticsearchSink;
////            } else {
////                synchronized (ElasticsearchSink.class) {
////                    elasticsearchSink = new ElasticsearchSink(clusterName, esHostTcpList);
////                    elasticsearchSinkList.add(elasticsearchSink);
////                }
////            }
//        } else {
        if (elasticsearchSink == null) {
            synchronized (ElasticsearchSink.class) {
                if (elasticsearchSink == null) {
                    elasticsearchSink = new ElasticsearchSink(clusterName, esHostTcpList);
//                    elasticsearchSinkList.add(elasticsearchSink);
                }
            }
        }
        return elasticsearchSink;
    }

    @Override
    public void insertBatch(String indexName, String type, Map<String, Map<String, String>> resultMap
            , Integer bulkActions, String bulkSize, Integer flushInterval, Integer concurrentRequests, Integer exponentialBackoff, Integer maxNumberOfRetries, Integer awaitCloseTimeout) {
        long t1 = System.currentTimeMillis();

        if (bulkSize.contains("g")) {
            byteSizeValue = new ByteSizeValue(Integer.valueOf(bulkSize.split("g")[0]), ByteSizeUnit.GB);
        } else if (bulkSize.contains("G")) {
            byteSizeValue = new ByteSizeValue(Integer.valueOf(bulkSize.split("G")[0]), ByteSizeUnit.GB);
        } else if (bulkSize.contains("m")) {
            byteSizeValue = new ByteSizeValue(Integer.valueOf(bulkSize.split("m")[0]), ByteSizeUnit.MB);
        } else if (bulkSize.contains("M")) {
            byteSizeValue = new ByteSizeValue(Integer.valueOf(bulkSize.split("M")[0]), ByteSizeUnit.MB);
        }

        Settings esSettings = Settings.builder()
                .put("cluster.name", clusterName)
                .put("client.transport.sniff", false).build();
        TransportClient client = new PreBuiltTransportClient(esSettings);
        client.addTransportAddresses(initTranSportAddress());

        // 优化批量执行
        BulkProcessor bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            //TODO beforeBulk会在批量提交之前执行
            @Override
            public void beforeBulk(long l, org.elasticsearch.action.bulk.BulkRequest bulkRequest) {
                LOGGER.info("---尝试操作" + bulkRequest.numberOfActions() + "条数据---");
            }

            //TODO 第一个afterBulk会在批量成功后执行，可以跟beforeBulk配合计算批量所需时间
            @Override
            public void afterBulk(long l, org.elasticsearch.action.bulk.BulkRequest bulkRequest, BulkResponse bulkResponse) {
                System.out.println("---尝试操作" + bulkRequest.numberOfActions() + "条数据成功---");
            }

            //TODO 第二个afterBulk会在批量失败后执行
            @Override
            public void afterBulk(long l, org.elasticsearch.action.bulk.BulkRequest bulkRequest, Throwable throwable) {
                System.out.println("---尝试操作" + bulkRequest.numberOfActions() + "条数据失败---");
            }
        })
                // 1w次请求执行一次bulk
                .setBulkActions(bulkActions)
                // 1gb的数据刷新一次bulk
                .setBulkSize(byteSizeValue)
                // 固定1s必须刷新一次
                .setFlushInterval(TimeValue.timeValueSeconds(flushInterval))
                // 并发请求数量, 0不并发, 1并发允许执行
                .setConcurrentRequests(concurrentRequests)
                // 设置退避, 100ms后执行, 最大请求3次
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueSeconds(exponentialBackoff), maxNumberOfRetries))
                .build();
        try {

            MultiGetRequestBuilder multiGetRequestBuilder = client.prepareMultiGet();

            ArrayList<String> keylist = new ArrayList<>();
            keylist.addAll(resultMap.keySet());

            MultiGetRequestBuilder add = multiGetRequestBuilder.add(indexName, type, keylist);
            MultiGetResponse multiGetItemResponses = add.get();
            for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
                GetResponse response = itemResponse.getResponse();

                String id = response.getId();
                Map<String, String> insertInfoMap = resultMap.get(id);
                UpdateRequest updateRequest = new UpdateRequest(indexName, type, id)
                        .doc(insertInfoMap)
                        .docAsUpsert(true);
                bulkProcessor.add(updateRequest);
            }
            // 优化批量执行
            bulkProcessor.flush();
            boolean b = bulkProcessor.awaitClose(awaitCloseTimeout, TimeUnit.MINUTES);
            bulkProcessor.close();
            LOGGER.warn("返回的code值为：" + b);

            long t2 = System.currentTimeMillis();
            System.out.println("耗时: " + (t2 - t1) + "毫秒");
        } catch (Exception e1) {
            LOGGER.error("ERROR for synch2Elasticsearch() with liusinan(P6008527)", e1);
        } finally {
            client.close();
        }
    }

    /**
     * @return org.elasticsearch.common.transport.TransportAddress[]
     * @title initTranSportAddress
     * @Description 从esHostTcpList中循环获取（ip+端口号）并创建对应TransportAddress，添加到TransportAddress类型的数组中返回。
     * @author liusinan
     * @date 2020/7/31 下午7:12
     **/
    private static TransportAddress[] initTranSportAddress() {
        TransportAddress[] transportAddresses = new TransportAddress[esHostTcpList.size()];
        int offset = 0;
        for (int i = 0; i < esHostTcpList.size(); i++) {
            String[] ipHost = esHostTcpList.get(i).split(":");
            try {
                transportAddresses[offset] = new TransportAddress(InetAddress.getByName(ipHost[0].trim()), Integer.valueOf(ipHost[1].trim()));
                offset++;
            } catch (Exception e) {
                LOGGER.error("exec init transport address error:", e);
            }
        }
        return transportAddresses;
    }

    @Override
    public void deleteAllLineWithId(String id, String indexName, String type) {
        Settings esSettings = Settings.builder()
                .put("cluster.name", clusterName)
                .put("client.transport.sniff", false).build();
        TransportClient client = new PreBuiltTransportClient(esSettings);
        client.addTransportAddresses(initTranSportAddress());

        //delete
        //删除一整行（），通过key删除。
        try {
            DeleteRequestBuilder deleteRequestBuilder = client.prepareDelete(indexName, type, id);
            DeleteResponse deleteResponse = deleteRequestBuilder.get();
            deleteResponse.status();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            client.close();
        }
    }

    @Override
    public void deleteAllLineBatchWithIds(List<String> ids, String indexName, String type
            , Integer bulkActions, String bulkSize, Integer flushInterval, Integer concurrentRequests, Integer exponentialBackoff, Integer maxNumberOfRetries, Integer awaitCloseTimeout) {
        if (bulkSize.contains("g")) {
            byteSizeValue = new ByteSizeValue(Integer.valueOf(bulkSize.split("g")[0]), ByteSizeUnit.GB);
        } else if (bulkSize.contains("G")) {
            byteSizeValue = new ByteSizeValue(Integer.valueOf(bulkSize.split("G")[0]), ByteSizeUnit.GB);
        } else if (bulkSize.contains("m")) {
            byteSizeValue = new ByteSizeValue(Integer.valueOf(bulkSize.split("m")[0]), ByteSizeUnit.MB);
        } else if (bulkSize.contains("M")) {
            byteSizeValue = new ByteSizeValue(Integer.valueOf(bulkSize.split("M")[0]), ByteSizeUnit.MB);
        }

        Settings esSettings = Settings.builder()
                .put("cluster.name", clusterName)
                .put("client.transport.sniff", false).build();
        TransportClient client = new PreBuiltTransportClient(esSettings);
        client.addTransportAddresses(initTranSportAddress());

        // 优化批量执行
        BulkProcessor bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            //TODO beforeBulk会在批量提交之前执行
            @Override
            public void beforeBulk(long l, org.elasticsearch.action.bulk.BulkRequest bulkRequest) {
                LOGGER.info("---尝试操作" + bulkRequest.numberOfActions() + "条数据---");
            }

            //TODO 第一个afterBulk会在批量成功后执行，可以跟beforeBulk配合计算批量所需时间
            @Override
            public void afterBulk(long l, org.elasticsearch.action.bulk.BulkRequest bulkRequest, BulkResponse bulkResponse) {
                System.out.println("---尝试操作" + bulkRequest.numberOfActions() + "条数据成功---");
            }

            //TODO 第二个afterBulk会在批量失败后执行
            @Override
            public void afterBulk(long l, org.elasticsearch.action.bulk.BulkRequest bulkRequest, Throwable throwable) {
                System.out.println("---尝试操作" + bulkRequest.numberOfActions() + "条数据失败---");
            }
        })
                // 1w次请求执行一次bulk
                .setBulkActions(bulkActions)
                // 1gb的数据刷新一次bulk
                .setBulkSize(byteSizeValue)
                // 固定1s必须刷新一次
                .setFlushInterval(TimeValue.timeValueSeconds(flushInterval))
                // 并发请求数量, 0不并发, 1并发允许执行
                .setConcurrentRequests(concurrentRequests)
                // 设置退避, 100ms后执行, 最大请求3次
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueSeconds(exponentialBackoff), maxNumberOfRetries))
                .build();

        try {
            for (String id : ids) {
                DeleteRequest deleteRequest = new DeleteRequest(indexName, type, id);
                bulkProcessor.add(deleteRequest);
            }
            bulkProcessor.flush();
            boolean b = bulkProcessor.awaitClose(awaitCloseTimeout, TimeUnit.MINUTES);
            LOGGER.info("返回的code值为：" + b);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            client.close();
        }

    }

    @Override
    public void deleteCellBatch(List<String> ids, List<String> columnNames, String indexName, String type
            , Integer bulkActions, String bulkSize, Integer flushInterval, Integer concurrentRequests, Integer exponentialBackoff, Integer maxNumberOfRetries, Integer awaitCloseTimeout) {
        if (bulkSize.contains("g")) {
            byteSizeValue = new ByteSizeValue(Integer.valueOf(bulkSize.split("g")[0]), ByteSizeUnit.GB);
        } else if (bulkSize.contains("G")) {
            byteSizeValue = new ByteSizeValue(Integer.valueOf(bulkSize.split("G")[0]), ByteSizeUnit.GB);
        } else if (bulkSize.contains("m")) {
            byteSizeValue = new ByteSizeValue(Integer.valueOf(bulkSize.split("m")[0]), ByteSizeUnit.MB);
        } else if (bulkSize.contains("M")) {
            byteSizeValue = new ByteSizeValue(Integer.valueOf(bulkSize.split("M")[0]), ByteSizeUnit.MB);
        }

        Settings esSettings = Settings.builder()
                .put("cluster.name", clusterName)
                .put("client.transport.sniff", false).build();
        TransportClient client = new PreBuiltTransportClient(esSettings);
        client.addTransportAddresses(initTranSportAddress());

        // 优化批量执行
        BulkProcessor bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            //TODO beforeBulk会在批量提交之前执行
            @Override
            public void beforeBulk(long l, org.elasticsearch.action.bulk.BulkRequest bulkRequest) {
                LOGGER.info("---尝试操作" + bulkRequest.numberOfActions() + "条数据---");
            }

            //TODO 第一个afterBulk会在批量成功后执行，可以跟beforeBulk配合计算批量所需时间
            @Override
            public void afterBulk(long l, org.elasticsearch.action.bulk.BulkRequest bulkRequest, BulkResponse bulkResponse) {
                System.out.println("---尝试操作" + bulkRequest.numberOfActions() + "条数据成功---");
            }

            //TODO 第二个afterBulk会在批量失败后执行
            @Override
            public void afterBulk(long l, org.elasticsearch.action.bulk.BulkRequest bulkRequest, Throwable throwable) {
                System.out.println("---尝试操作" + bulkRequest.numberOfActions() + "条数据失败---");
            }
        })
                // 1w次请求执行一次bulk
                .setBulkActions(bulkActions)
                // 1gb的数据刷新一次bulk
                .setBulkSize(byteSizeValue)
                // 固定1s必须刷新一次
                .setFlushInterval(TimeValue.timeValueSeconds(flushInterval))
                // 并发请求数量, 0不并发, 1并发允许执行
                .setConcurrentRequests(concurrentRequests)
                // 设置退避, 100ms后执行, 最大请求3次
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueSeconds(exponentialBackoff), maxNumberOfRetries))
                .build();

        //删除多个单元格，通过插入的方式更新，删除位置变成空值插入。
        HashMap<String, String> source = new HashMap<>();
        try {
            for (String id : ids) {
                for (String columnName : columnNames) {
                    source.put(columnName, "");
                    UpdateRequest updateRequest = new UpdateRequest(indexName, type, id)
                            .doc(source);
                    bulkProcessor.add(updateRequest);
                }
            }

            bulkProcessor.flush();
            boolean b = bulkProcessor.awaitClose(awaitCloseTimeout, TimeUnit.MINUTES);
            LOGGER.info("返回的code值为：" + b);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            client.close();
        }
    }

}
