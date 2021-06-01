package com.moji.flink;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 单词计算之离线计算
 * @author feiliang.wang
 */
public class BatchWordCountJava {

  public static void main(String[] args) throws Exception {
    String inputPath="/Users/feiliang.wang/ideaworkspace/bigdata/a";
    String outPath="/Users/feiliang.wang/ideaworkspace/bigdata/result";
    //获取Fink的运行环境
    StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
    //读取文件中的内容
    DataStreamSource<String> text=env.readTextFile(inputPath);

    SingleOutputStreamOperator<Tuple2<String,Integer>> counts=text.flatMap(new Tokenizer()).keyBy(0).sum(1);
    counts.writeAsCsv(outPath, WriteMode.NO_OVERWRITE,"\n"," ").setParallelism(1);
    env.execute("batch word count");
  }


  public static class Tokenizer implements FlatMapFunction<String,Tuple2<String,Integer>>{
    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
       String[] tokens=value.toLowerCase().split("\\W+");
       for(String token:tokens){
         if(token.length()>0){
           out.collect(new Tuple2<String, Integer>(token,1));
         }
       }
    }
  }

}
