# Es导入工具类

## 插入数据

```java
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
public void insert() {
        HashMap<String, Map<String, String>> resultMap = new HashMap<>();

        Random random = new Random();
        Random agerandom = new Random();
        Random sexrandom = new Random();
        String[] str = new String[]{"male", "female"};

        for (int i = 1; i <= 1000000; i++) {
            HashMap<String, String> map = new HashMap<>();
            map.put("Cno", String.valueOf(i));
            map.put("Cname", ("customer" + i));
            map.put("Cage", String.valueOf(agerandom.nextInt(100)));
            map.put("Csex", str[sexrandom.nextInt(1)]);
            resultMap.put(String.valueOf(i), map);
//            map.clear();
        }

        System.out.println(resultMap.size());

        String clusterName = "my-es";
        List<String> esHostInfo = new ArrayList<>();
        esHostInfo.add("localhost:9300");
        ElasticsearchSink elasticsearchSink = ElasticsearchSink.getInstance(clusterName, esHostInfo);

//        String clusterName1 = "my-es1";
//        List<String> esHostInfo1 = new ArrayList<>();
//        esHostInfo1.add("localhost:93001");
//        ElasticsearchSink elasticsearchSink1 = ElasticsearchSink.getInstance(clusterName1, esHostInfo1);
//
//        String clusterName2 = "my-es";
//        List<String> esHostInfo2 = new ArrayList<>();
//        esHostInfo2.add("localhost:9300");
//        ElasticsearchSink elasticsearchSink2 = ElasticsearchSink.getInstance(clusterName2, esHostInfo2);
//
//        System.out.println(elasticsearchSink == elasticsearchSink1);
//        System.out.println(elasticsearchSink1 == elasticsearchSink2);
//        System.out.println(elasticsearchSink == elasticsearchSink2);

        String indexName = "es_index";
        String type = "_doc";
    
        elasticsearchSink.insertBatch(indexName, type, resultMap, 100000, "1g", 1, 8, 1, 3, 1);

    }
```

ElasticsearchSink.getInstance(clusterName, esHostInfo)传入es的集群名称<String>和ES集群的所有host信息（192.168.1.1:9300）<List<String>>

## 删除数据

```java
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
public void deleteAllLineWithIdTest() {
        try {
            long t1 = System.currentTimeMillis();

            String clusterName = "my-es";

            List<String> esHostInfo = new ArrayList<>();
            esHostInfo.add("localhost:9300");

            ElasticsearchSink elasticsearchSink = ElasticsearchSink.getInstance(clusterName, esHostInfo);

            String id = "1";
            String indexName = "es_index";
            String type = "_doc";

            elasticsearchSink.deleteAllLineWithId(id, indexName, type);

            long t2 = System.currentTimeMillis();
            System.out.println("删除id为" + id + "的一整行，耗时: " + (t2 - t1) + "毫秒");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
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
public void deleteAllLineBatchWithIdsTest() {
        try {
            long t1 = System.currentTimeMillis();

            String clusterName = "my-es";

            List<String> esHostInfo = new ArrayList<>();
            esHostInfo.add("localhost:9300");

            ElasticsearchSink elasticsearchSink = ElasticsearchSink.getInstance(clusterName, esHostInfo);

            List<String> idList = new ArrayList<>();
            for (int i = 2; i < 11; i++) {
                idList.add(String.valueOf(i));
            }

            String indexName = "es_index";
            String type = "_doc";

            elasticsearchSink.deleteAllLineBatchWithIds(idList, indexName, type, 50000, "10m", 1, 8, 1, 3, 1);

            long t2 = System.currentTimeMillis();
            System.out.println("删除多行，耗时: " + (t2 - t1) + "毫秒");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

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
public void deleteCellBatchTest() {
        try {
            long t1 = System.currentTimeMillis();

            String clusterName = "my-es";

            List<String> esHostInfo = new ArrayList<>();
            esHostInfo.add("localhost:9300");

            ElasticsearchSink elasticsearchSink = ElasticsearchSink.getInstance(clusterName, esHostInfo);

            List<String> idList = new ArrayList<>();
            for (int i = 11; i < 100001; i++) {
                idList.add(String.valueOf(i));
            }

            List<String> columnNames = new ArrayList<>();
            columnNames.add("Cno");

            String indexName = "es_index";
            String type = "_doc";

            elasticsearchSink.deleteCellBatch(idList, columnNames, indexName, type, 50000, "10m", 1, 8, 1, 3, 1);

            long t2 = System.currentTimeMillis();
            System.out.println("删除多行，耗时: " + (t2 - t1) + "毫秒");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
```

## 参数设置推荐

```
indexName          索引名
type               类型（_doc）
resultMap          插入的数据，通过id(index)关联起来的HashMap<String, Map<String, String>> ： <id, 要插入当前行的列和对应列的值>
bulkActions        ?次请求执行一次bulk
bulkSize           数据达到?大小刷新一次bulk（单位：g,G,m,M,mb,MB） 例如：200MB，1g
flushInterval      固定？s刷新一次
concurrentRequests 并发请求数量, 0不并发, 1并发允许执行，需要测试，根据cpu来看，太多性能会下降。
exponentialBackoff 设置退避, ？s后执行,
maxNumberOfRetries 退避最大请求?次
awaitCloseTimeout  关闭处理器的超时时间（分钟为单位）：
 如果启用了按时间刷新，则将其关闭。将清除所有剩余的批量操作
 如果未启用并发请求，则立即返回{@code true}
 如果启用了并发请求，则等待所有指定的请求完成直到指定的超时，然后返回{@code true}
 如果指定的等待时间在所有指定的请求完成之前已经过，则返回{@code false}
```

**resultMap**：你可能会在mapreduce中或者普通的java程序中使用，要注意一个map传入的大小，写入一个map，当size等于特定值就传入插入方法。

**bulkActions**：设置为5000或者1000，表示多少条执行一次批量插入，自己衡量，不宜过大不宜过小。

**bulkSize**：注意单位，调成10M或者大一点，网上资料显示不宜超过10m，自己测试。

**flushInterval**：表示多少秒后刷新一次，推荐为1。

**concurrentRequests**：设置为0表示1个请求并行，设置为1表示2个请求并行，设置为cpu core数量或者更多的值，如果你了解并发，你会知道调的过大会伤害硬件，过小会很慢，自己衡量测试。

**exponentialBackoff**：设置自定义退避策略。退避策略定义了批量处理器应如何在内部处理批量请求的重试，以防万一由于资源限制（线程池已满）而失败。设置为1s等待时长，重试3次，可以设置大些防止失败。（注意设置1s，3次，等待时长为3s，可以修改代码，修改成毫秒级别，重试多次）

**awaitCloseTimeout**：关闭处理器的超时时间（分钟为单位）：

 **如果启用了按时间刷新，则将其关闭。将清除所有剩余的批量操作
 如果未启用并发请求，则立即返回{@code true}
 如果启用了并发请求，则等待所有指定的请求完成直到指定的超时，然后返回{@code true}
 如果指定的等待时间在所有指定的请求完成之前已经过，则返回{@code false}**

# 并发异步提交，参数至关重要，查阅资料阅读源码后调试参数！！！

链接:https://pan.baidu.com/s/1DedAk7KZqZv_g4qkbRWg0w 提取码:z8d6

点个start：https://github.com/Denovo1998/elasticsearch-sinker