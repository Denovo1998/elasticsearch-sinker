package com.pactera.bigdata.elasticsearch.sinker.synch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author liusinan
 * @version 1.0.0
 * @ClassName ElasticsearchImport.java
 * @Description TODO
 * @createTime 2020年08月08日 15:18:00
 */
public interface ElasticsearchImport {

    /**
     * 批量插入（更新）
     *
     * @param indexName          索引名
     * @param type               类型（_doc）
     * @param resultMap          插入的数据，通过id(index)关联起来的HashMap<String, Map<String, String>> ： <id, 要插入当前行的列和对应列的值>
     * @param bulkActions        ?次请求执行一次bulk
     * @param bulkSize           数据达到?大小刷新一次bulk（单位：g,G,m,M,mb,MB） 例如：200MB，1g
     * @param flushInterval      固定？s刷新一次
     * @param concurrentRequests 并发请求数量, 0不并发, 1并发允许执行，需要测试，根据cpu来看，太多性能会下降。
     * @param exponentialBackoff 设置退避, ？s后执行,
     * @param maxNumberOfRetries 退避最大请求?次
     * @param awaitCloseTimeout  关闭处理器的超时时间（分钟为单位）：
     *                           如果启用了按时间刷新，则将其关闭。将清除所有剩余的批量操作
     *                           如果未启用并发请求，则立即返回{@code true}
     *                           如果启用了并发请求，则等待所有指定的请求完成直到指定的超时，然后返回{@code true}
     *                           如果指定的等待时间在所有指定的请求完成之前已经过，则返回{@code false}
     * @title insertBatch
     * @author liusinan
     * @date 2020/8/31 下午8:52
     **/
    void insertBatch(String indexName, String type, Map<String, Map<String, String>> resultMap
            , Integer bulkActions, String bulkSize, Integer flushInterval, Integer concurrentRequests, Integer exponentialBackoff, Integer maxNumberOfRetries, Integer awaitCloseTimeout);

    /**
     * 删除一行
     *
     * @param id        要删除行的ID（<String>）
     * @param indexName 索引名称
     * @param type      类型（_doc）
     * @title deleteAllLineWithId
     * @author liusinan
     * @date 2020/8/31 下午8:50
     **/
    void deleteAllLineWithId(String id, String indexName, String type);

    /**
     * 批量删除一行
     *
     * @param ids                要删除行的ID（List<String>）
     * @param indexName          索引名称
     * @param type               类型（_doc)
     * @param bulkActions        ?次请求执行一次bulk
     * @param bulkSize           数据达到?大小刷新一次bulk（单位：g,G,m,M,mb,MB） 例如：200MB，1g
     * @param flushInterval      固定？s刷新一次
     * @param concurrentRequests 并发请求数量, 0不并发, 1并发允许执行，需要测试，根据cpu来看，太多性能会下降。
     * @param exponentialBackoff 设置退避, ？s后执行,
     * @param maxNumberOfRetries 退避最大请求?次
     * @param awaitCloseTimeout  关闭处理器的超时时间（分钟为单位）：
     *                           如果启用了按时间刷新，则将其关闭。将清除所有剩余的批量操作
     *                           如果未启用并发请求，则立即返回{@code true}
     *                           如果启用了并发请求，则等待所有指定的请求完成直到指定的超时，然后返回{@code true}
     *                           如果指定的等待时间在所有指定的请求完成之前已经过，则返回{@code false}
     * @title deleteAllLineBatchWithIds
     * @author liusinan
     * @date 2020/8/31 下午8:47
     **/
    void deleteAllLineBatchWithIds(List<String> ids, String indexName, String type
            , Integer bulkActions, String bulkSize, Integer flushInterval, Integer concurrentRequests, Integer exponentialBackoff, Integer maxNumberOfRetries, Integer awaitCloseTimeout);

    /**
     * 批量删除单元格
     *
     * @param ids                要删除行的ID（List<String>）
     * @param columnNames        指定要删除行的哪几列
     * @param indexName          索引名称
     * @param type               类型（_doc）
     * @param bulkActions        ?次请求执行一次bulk
     * @param bulkSize           数据达到?大小刷新一次bulk（单位：g,G,m,M,mb,MB） 例如：200MB，1g
     * @param flushInterval      固定？s刷新一次
     * @param concurrentRequests 并发请求数量, 0不并发, 1并发允许执行，需要测试，根据cpu来看，太多性能会下降。
     * @param exponentialBackoff 设置退避, ？s后执行,
     * @param maxNumberOfRetries 退避最大请求?次
     * @param awaitCloseTimeout  关闭处理器的超时时间（分钟为单位）：
     *                           如果启用了按时间刷新，则将其关闭。将清除所有剩余的批量操作
     *                           如果未启用并发请求，则立即返回{@code true}
     *                           如果启用了并发请求，则等待所有指定的请求完成直到指定的超时，然后返回{@code true}
     *                           如果指定的等待时间在所有指定的请求完成之前已经过，则返回{@code false}
     * @title deleteCellBatch
     * @author liusinan
     * @date 2020/8/11 下午11:44
     **/
    void deleteCellBatch(List<String> ids, List<String> columnNames, String indexName, String type
            , Integer bulkActions, String bulkSize, Integer flushInterval, Integer concurrentRequests, Integer exponentialBackoff, Integer maxNumberOfRetries, Integer awaitCloseTimeout);
}
