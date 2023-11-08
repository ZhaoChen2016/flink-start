/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jd.flink.job;

import com.jd.bdp.rrd.apus.flink.sdk.kafka.KafkaParameters;
import com.jd.bdp.rrd.apus.flink.sdk.utils.KafkaSecurityProviderCleaner;
import com.jd.bdp.rrd.apus.flink.sdk.utils.RichFunctionProxy;
import com.jd.flink.constant.Constants;
import com.jd.flink.pojo.AggKey;
import com.jd.flink.pojo.AggValue;
import com.jd.flink.utils.CommonUtil;
import com.jd.flink.utils.ConfigUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPunctuatedWatermarksAdapter;
import org.apache.flink.util.Collector;
import com.jd.flink.pojo.AggResult;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 *  * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 *
 *           Here, you can start creating your execution plan for Flink.
 *
 */
@Slf4j
public class TopUrl {

    public static void main(String[] args) throws Exception {
        // 读配置文件
        if (args.length != 2){
            log.info("param count must be 2");
        }
        if ("topurl".equals(args[0])){
            ConfigUtil.loadProperties("topurl.properties");
        }
        int slot = Integer.valueOf(args[1]);
        // set up the streaming execution environment获取执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(slot);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)));
/*      ExecutionConfig config = new ExecutionConfig();
        config 生成watermark的时间
        config.setAutoWatermarkInterval(10);*/
        checkpointConfig(env);

        /*获取kafkaconsumer 可自定义反序列化 -> 在kafkasource抽取watermark*/
        KafkaParameters jdqKP = KafkaParameters.builder()
                .user(Constants.USERNAME)
                .password(Constants.CONSUMER_PASSWORD)
                .clientId(Constants.CONSUMER_CLIENTID)
                .topicList(Collections.singletonList(Constants.CONSUMER_TOPIC))
                .build();
        jdqKP.selfValidate();

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                jdqKP.getTopicList(),
                new SimpleStringSchema(),
                jdqKP.properties4Consumer()
        );
        SourceFunction<String> sourceFunction = (SourceFunction) RichFunctionProxy.getRichFunctionProxy(consumer, new KafkaSecurityProviderCleaner());

/*        Properties kafkaConsumerProp = new Properties();
        kafkaConsumerProp.setProperty("bootstrap.servers", ConfigUtil.kafkaBroker);
        kafkaConsumerProp.setProperty("group.id", ConfigUtil.kafkaConsumerGroup);
        kafkaConsumerProp.setProperty("flink.partition-discovery.interval-millis", "30000");//动态发现 间隔时间
        kafkaConsumerProp.setProperty("auto.offset.reset", "latest");
//        kafkaConsumerProp.setProperty("client.id",prop.getProperty("client.id"));
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(ConfigUtil.kafkaSourceTopic, new SimpleStringSchema(), kafkaConsumerProp);*/
        /*
        默认提交offset配置：
        consumer.setStartFromGroupOffsets();//从检查点重启时 任务从checkpoint恢复 与此配置无关
        consumer.setCommitOffsetsOnCheckpoints(true);//默认true 检查点提交时提交offset
        */

        DataStream<String> text = env.addSource(sourceFunction).uid("kafkaSource").name("kafkaSource");

        SingleOutputStreamOperator<String> topStream = text.map(new Analysers()).name("analyser").uid("analyser")
                .filter(x->x.isPresent()).name("filter").uid("filter")
                .map(x->x.get()).name("get").uid("get")
                .returns(TypeInformation.of(new TypeHint<Tuple2<AggKey, AggValue>>(){}))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<AggKey, AggValue>>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((tuple, l) ->tuple.f0.timeStamp)).name("watermark").uid("watermark")
                .keyBy(x->x.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new CountAgg(),new ResultAgg()).uid("windowAggregate").name("windowAggregate")//和keyBy中的key保持一致
                .keyBy(x->x.windowEnd)
                .process(new TopNHot(5)).uid("tophot").name("tophot")
                .map(x->x.toString()).uid("getStr").name("getStr")
                .setParallelism(1);
        /*默认分区器FlinkFixedPartitioner 不指定分区器会roundRobbin
        FlinkKafkaProducer011.Semantic.EXACTLY_ONCE 两阶段提交
        */
/*        Properties kafkaProducerProp = new Properties();
        kafkaProducerProp.setProperty("bootstrap.servers", ConfigUtil.kafkaBroker);
//        kafkaProducerProp.setProperty("client.id",prop.getProperty("client.id"));
//        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(prop.getProperty("sink.topic"),new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()),kafkaProducerProp,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(ConfigUtil.kafkaSinkTopic, new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()), kafkaProducerProp);
        topStream.addSink(myProducer).uid("kafkaSink").name("KafkaSink");*/

        KafkaParameters jdqProducer = KafkaParameters.builder()
                .user(Constants.USERNAME)
                .password(Constants.PRODUCER_PASSWORD)
                .clientId(Constants.PRODUCER_CLIENTID)
                .topicList(Collections.singletonList(Constants.PRODUCER_TOPIC))
                .build();
        jdqProducer.selfValidate();

        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
                Constants.PRODUCER_TOPIC,
                new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),
                jdqProducer.properties4Producer(),
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
        // 推荐设置为false。当某消息发送到kafka失败时,为true则flink只打印错误日志，可能会导致数据漏写到kafka。为false则flink job直接fail。
        producer.setLogFailuresOnly(false);
        // 一般推荐设置为false。若启动Event Time且该参数为true，则会将上游数据的事件时间(event time)写入到Kafka的ProducerRecord中。用户可根据自己需求设置该参数。
        producer.setWriteTimestampToKafka(false);

        // 修复潜在Kafka NoClassDefFoundError问题，推荐所有用户按照如下设置，对任务无影响，详情参见https://cf.jd.com/pages/viewpage.action?pageId=272119157
        SinkFunction<String> sinkFunction = (SinkFunction) RichFunctionProxy.getRichFunctionProxy(producer, new KafkaSecurityProviderCleaner());
        topStream.addSink(sinkFunction).name("jdq sink");

        // execute program触发程序执行
        env.execute("Flink Streaming Java API Skeleton");
    }

    private static void checkpointConfig(StreamExecutionEnvironment env){
        env.enableCheckpointing(600000,CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(600000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(300000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend(Constants.HDFS_CHECKPOINT_PATH));
    }


    private static class Analysers implements MapFunction<String, Optional<Tuple2<AggKey,AggValue>>>{

        @Override
        public Optional<Tuple2<AggKey, AggValue>> map(String s) throws Exception {
            try {
                ArrayList<String> columns = CommonUtil.split(s, Constants.LOG_DELIMITER, Constants.LOG_END ,90);
                int minSize = 35;
                if (columns.size() < minSize) {
                    return Optional.ofNullable(null);
                }
                AggKey key = new AggKey(Long.parseLong(columns.get(Constants.LOG_MSEC_INDEX))*1000,columns.get(Constants.LOG_DOMAIN_INDEX),columns.get(Constants.LOG_URL_INDEX));
                AggValue value = new AggValue(Integer.parseInt(columns.get(Constants.LOG_FLOW_INDEX)));
                System.out.println("mesc is" + key.timeStamp);
                return Optional.ofNullable(new Tuple2<>(key,value));
            }catch (Exception e){
                e.printStackTrace();
                return Optional.ofNullable(null);
            }

        }
    }

    private static class CountAgg implements AggregateFunction<Tuple2<AggKey,AggValue>,AggValue,AggValue>{

        @Override
        public AggValue createAccumulator() {
            return new AggValue(0);
        }

        @Override
        public AggValue add(Tuple2<AggKey, AggValue> tuple2, AggValue aggValue) {
            return new AggValue(tuple2.f1.flow+aggValue.flow);
        }

        @Override
        public AggValue getResult(AggValue aggValue) {
            return aggValue;
        }

        @Override
        public AggValue merge(AggValue aggValue, AggValue acc1) {
            return new AggValue(aggValue.flow+acc1.flow);
        }
    }

    private static class ResultAgg extends ProcessWindowFunction<AggValue, AggResult, AggKey, TimeWindow> {
        @Override
        public void process(AggKey aggKey, Context context, Iterable<AggValue> iterable, Collector<AggResult> out) throws Exception {
            System.out.println("key is: "+aggKey.toString()+" current watermark :["+context.currentWatermark()+"]"+ "current window: [start: "+context.window().getStart()+", end: "+context.window().getEnd()+"]");
            out.collect(new AggResult(iterable.iterator().next(),context.window().getEnd(),aggKey));
        }
    }

    private static class TopNHot extends ProcessFunction<AggResult, AggResult> {
        private int topn;
        private ListState<AggResult> listState;

        public TopNHot(int topn) {
            this.topn = topn;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(new ListStateDescriptor<AggResult>("url_list_state", Types.POJO(AggResult.class)));
        }

        @Override
        public void processElement(AggResult aggResult, Context context, Collector<AggResult> out) throws Exception {
            listState.add(aggResult);
            System.out.println("result is: "+ aggResult);
            context.timerService().registerEventTimeTimer(aggResult.windowEnd+1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AggResult> out) throws Exception {
            List<AggResult> topList = new ArrayList<>();
            for(AggResult aggResult:listState.get()){
                topList.add(aggResult);
            }
            listState.clear();
            topList.sort(new Comparator<AggResult>() {
                @Override
                public int compare(AggResult o1, AggResult o2) {
                    return o2.tuple2.flow-o1.tuple2.flow;
                }
            });
            List<AggResult> sortList;
            if(topList.size()>topn){
                sortList = topList.subList(0,topn-1);
            }else{
                sortList= topList;
            }
            for (AggResult aggResult:sortList){
                out.collect(aggResult);
            }
        }
    }


}
