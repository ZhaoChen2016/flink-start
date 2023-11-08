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

import com.jd.flink.constant.Constants;
import com.jd.flink.pojo.AggKey;
import com.jd.flink.pojo.AggResult;
import com.jd.flink.pojo.AggValue;
import com.jd.flink.utils.CommonUtil;
import com.jd.flink.utils.ConfigUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
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
 */
@Slf4j
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment获取执行环境
//		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		// 读配置文件
		if (args.length != 2){
			log.info("param count must be 2");
		}
		if ("topurl".equals(args[0])){
			ConfigUtil.loadProperties("topurl.properties");
		}
		int slot = Integer.valueOf(args[1]);
		Configuration conf = new Configuration();
		conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		env.setParallelism(5);
		//设置重启策略
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)));
		//设置checkpoint间隔和模式
		env.enableCheckpointing(300_000, CheckpointingMode.EXACTLY_ONCE);
		//设置checkpoint超时时间
		env.getCheckpointConfig().setCheckpointTimeout(600_000);
		// 同一时间只允许进行一个检查点
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
		// 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
		env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

		env.setStateBackend(new FsStateBackend("file:///E:\\flink\\flink-user-demo\\flink-start\\src\\main\\java\\checkpoint",true));

//		env.setRuntimeMode(RuntimeExecutionMode.BATCH);
//		DataStreamSource<String> text = env.readTextFile("E:\\flink\\flink-user-demo\\flink-start\\text.log");
		DataStreamSource<String> text = env.socketTextStream("localhost", 9999, '\n');
		text.map(new Analysers())
				.filter(x->x.isPresent())
				.map(x->x.get())
				.returns(TypeInformation.of(new TypeHint<Tuple2<AggKey, AggValue>>(){}))//有泛型，类型擦除，lambda表达式不能推断出类型
				.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<AggKey, AggValue>>forBoundedOutOfOrderness(Duration.ofSeconds(10))
						.withTimestampAssigner((tuple, l) ->tuple.f0.timeStamp))
				.keyBy(x->x.f0)
				.window(TumblingEventTimeWindows.of(Time.seconds(10)))
				.aggregate(new CountAgg(),new ResultAgg())//和keyBy中的key保持一致
				.keyBy(x->x.windowEnd)//按照windowend时间分流，算window时间内的topurl
				.process(new TopNHot(5))
		.print();

//		System.out.println(env.getExecutionPlan());
		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * https://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */

		// execute program触发程序执行
		env.execute("Flink Streaming Java API Skeleton");
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
