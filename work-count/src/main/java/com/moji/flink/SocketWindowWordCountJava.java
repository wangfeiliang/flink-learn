package com.moji.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 单词计数之滑动窗口计算
 */
public class SocketWindowWordCountJava {

  public static void main(String[] args) {

    int port;
    try{
      ParameterTool parameterTool=ParameterTool.fromArgs(args);
      port=parameterTool.getInt("port");
    }catch (Exception e){
      port=9000;
    }

    //获取Fink的运行环境
    StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
    String hostname="127.0.0.1";
    String delimiter="\n";

    //链接socket获取输入数据
    DataStreamSource<String> text = env.socketTextStream(hostname,port,delimiter);

    DataStream<WordWithCount> windowCounts=text.flatMap(new FlatMapFunction<String, WordWithCount>() {
      @Override
      public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
        String[] splits=value.split("\\s");
        for (String word:splits){
          out.collect(new WordWithCount(word,1) );
        }
      }   //指定时间窗口大小为2s,指定时间间隔为1s
    }).keyBy("word").timeWindow(Time.seconds(5),Time.seconds(1)).sum("count");

    //把数据打印到控制台并且设置并行度
    windowCounts.print().setParallelism(1);
    try {
      //这一行代码一定要实现,否则程序不执行
      env.execute("Socket window count");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static class WordWithCount{
    public String word;
    public long count;
    public WordWithCount(){

    }

    public WordWithCount(String word, long count) {
      this.word = word;
      this.count = count;
    }

    @Override
    public String toString() {
      return "WordWithCount{"+"word='"+word+"\'"+"count="+count+"}";
    }

  }





}
