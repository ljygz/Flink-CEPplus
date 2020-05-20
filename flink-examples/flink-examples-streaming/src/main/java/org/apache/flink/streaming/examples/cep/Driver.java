package org.apache.flink.streaming.examples.cep;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.RichPatternSelectFunction;
import org.apache.flink.cep.listern.CepListener;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.RichIterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;


import java.util.ArrayList;
import java.util.Map;

/**
 * @Description:
 * @Author: greenday
 * @Date: 2019/8/15 14:05
 */
public class Driver {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//		数据源
		SingleOutputStreamOperator<Tuple3<String, Long, String>> source = env.fromElements(
			  new Tuple3<String, Long, String>("a", 1000000001000L, "22")
			, new Tuple3<String, Long, String>("b", 1000000002000L, "23")
			, new Tuple3<String, Long, String>("c", 1000000003000L, "23")
			, new Tuple3<String, Long, String>("d", 1000000003000L, "23")
			, new Tuple3<String, Long, String>("change", 1000000003001L, "23")
			, new Tuple3<String, Long, String>("e", 1000000004000L, "24")
			, new Tuple3<String, Long, String>("f", 1000000005000L, "23")
			, new Tuple3<String, Long, String>("g", 1000000006000L, "23")
		).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple3<String, Long, String>>() {
			long maxTimsStamp;

			public Watermark checkAndGetNextWatermark(Tuple3<String, Long, String> stringLongStringTuple3, long l) {
				return new Watermark(maxTimsStamp - 1000);
			}

			public long extractTimestamp(Tuple3<String, Long, String> stringLongStringTuple3, long per) {
				long elementTime = stringLongStringTuple3.f1;
				if (elementTime > maxTimsStamp) {
					maxTimsStamp = elementTime;
				}
				return elementTime;
			}
		});

		Pattern<Tuple3<String, Long, String>,?> pattern = Pattern
			.<Tuple3<String, Long, String>>begin("start").where(new RichIterativeCondition<Tuple3<String, Long, String>>() {
				@Override
				public boolean filter(Tuple3<String, Long, String> value, Context<Tuple3<String, Long, String>> ctx) throws Exception {
					return value.f0.equals("a");
				}
			})
			.next("middle").where(new RichIterativeCondition<Tuple3<String, Long, String>>() {
				@Override
				public boolean filter(Tuple3<String, Long, String> value, Context<Tuple3<String, Long, String>> ctx) throws Exception {
					return value.f0.equals("b");
				}
			})
			.within(Time.minutes(5));

		PatternStream patternStream = CEP.pattern(source, pattern);

		PatternStream patternstream = patternStream
//			更新逻辑
			.registerListener(new CepListener<Tuple3<String, Long, String>>(){
				@Override
				public Boolean needChange(Tuple3<String, Long, String> element) {
					return element.f0.equals("change");
				}
				@Override
				public Pattern returnPattern(Tuple3<String, Long, String> flagElement) {
					System.out.println("接收到flag数据:"+flagElement.toString()+"感知到切换逻辑");
					Pattern<Tuple3<String, Long, String>, ?> pattern = Pattern
						.<Tuple3<String, Long, String>>begin("start").where(new RichIterativeCondition<Tuple3<String, Long, String>>() {
							@Override
							public boolean filter(Tuple3<String, Long, String> value, Context<Tuple3<String, Long, String>> ctx) throws Exception {
								return value.f0.equals("e");
							}
						})
						.next("middle").where(new RichIterativeCondition<Tuple3<String, Long, String>>() {
							@Override
							public boolean filter(Tuple3<String, Long, String> value, Context<Tuple3<String, Long, String>> ctx) throws Exception {
								return value.f0.equals("f");
							}
						})
						.next("end").where(new RichIterativeCondition<Tuple3<String, Long, String>>() {
							@Override
							public boolean filter(Tuple3<String, Long, String> value, Context<Tuple3<String, Long, String>> ctx) throws Exception {
								return value.f0.equals("g");
							}
						})
						.within(Time.minutes(5));
					return pattern;
				}
			});

		patternstream.select(new RichPatternSelectFunction<Tuple3<String, Long, String>, Tuple3<String,String,String>>() {
			@Override
			public Tuple3 select(Map pattern) throws Exception {
				String start =  pattern.containsKey("start") ? ((ArrayList)pattern.get("start")).get(0).toString() : "" ;
				String middle = pattern.containsKey("middle") ? ((ArrayList)pattern.get("middle")).get(0).toString() : "" ;
				String end = pattern.containsKey("end") ? ((ArrayList)pattern.get("end")).get(0).toString() : "" ;
				return new Tuple3<String,String,String>(start,middle,end);
			}
		}).print();

		env.execute("cep");
	}
}
