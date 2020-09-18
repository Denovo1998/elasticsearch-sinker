package com.pactera.bigdata.elasticsearch.test;

import com.pactera.bigdata.elasticsearch.sinker.synch.ElasticsearchSink;
import org.junit.Test;

import java.util.*;

/**
 * @author liusinan
 * @version 1.0.0
 * @ClassName EsTest.java
 * @Description TODO
 * @createTime 2020年08月11日 22:32:00
 */
public class EsTest {

    @Test
    public void insert() {
        /*JedisPool jedisPool = JedisPoolUtil.getJedisPoolInstance();

        Jedis jedis = jedisPool.getResource();
        HashMap<String, String> resultMap = new HashMap<>();
        try {
            // 游标初始值为0
            String cursor = ScanParams.SCAN_POINTER_START;
            String key = "*";
            ScanParams scanParams = new ScanParams();
            scanParams.match(key);
            scanParams.count(Integer.MAX_VALUE);
            while (true) {
                long t1 = System.currentTimeMillis();
                //使用scan命令获取数据，使用cursor游标记录位置，下次循环使用
                ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                // 返回0 说明遍历完成
                cursor = scanResult.getCursor();
                List<String> keys = scanResult.getResult();

                List<String> values = jedis.mget(keys.toArray(new String[keys.size()]));
                for (String value : values) {
                    String[] split = value.split(":");
                    String uid = split[0];
                    String guid = split[1];
                    resultMap.put(uid, guid);
                }
                long t2 = System.currentTimeMillis();
                System.out.println("从redis获取" + keys.size() + "条数据，耗时: " + (t2 - t1) + "毫秒,cursor:" + cursor);
                keys.clear();
                if ("0".equals(cursor)) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            JedisPoolUtil.release(jedisPool, jedis);
        }*/
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
        elasticsearchSink.insertBatch(indexName, type, resultMap
                , 100000, "1g", 1, 8, 1, 3, 1);

    }

    @Test
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

    @Test
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

    @Test
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
}
