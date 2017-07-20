package com.tencent.mikemyzhao;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * 
 * 处理一下cvs文件，从有time的位置截取文件
 * 
 */
public class Step1 {
	public static boolean run(Configuration config, Map<String,String> paths){
		
		try {
			FileSystem fs = FileSystem.get(config);
			Job job = Job.getInstance(config);
			
			job.setJobName("step1");
		
			job.setJarByClass(Step1.class);
			job.setMapperClass(Step1_Mapper.class);
			job.setReducerClass(Step1_Reducer.class);
		
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(NullWritable.class);
		
			//在Step1Input中指定输入文件
			FileInputFormat.addInputPath(job,new Path(paths.get("Step1Input")));
		
			//在Step1Output中指定输出文件
			Path outpath = new Path(paths.get("Step1Output"));
			if(fs.exists(outpath)){
				fs.delete(outpath,true);
			}
			FileOutputFormat.setOutputPath(job,outpath);
			
			boolean f = job.waitForCompletion(true);
			return f;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		return false;
	}
	
	
	static class Step1_Mapper extends Mapper<LongWritable,Text,Text,NullWritable>{
		long linecount = 0;
		public static enum FileRecorder{
			StatusRecorder,
			DataRecorder
		}
		@Override
		protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			//对value进行处理
			linecount = key.get();
			String[] strs = StringUtils.split(value.toString(),','); 
			context.getCounter(FileRecorder.DataRecorder).increment(1);
			FileRecorder test = FileRecorder.DataRecorder;
			//System.out.println("line count : "+test.toString());
			
			if (strs.length >= 3) {
				if (strs[0] != null && !strs[0].toString().equals("time") ) {
					System.out.println(strs[0].toString() + "-" + strs[3].toString() + "," + strs[4].toString());
					Double total = (Double.parseDouble(strs[3])) + Double.parseDouble(strs[4]);
					total = Double.parseDouble(String.format("%.3f",total));
					Text outputkey = new Text(strs[0].toString() + 
							"-" + strs[3].toString() + "," + strs[4].toString() + 
							"," + Double.toString(total));
					context.write(outputkey, NullWritable.get());
				}
			}
		}
	}
	
	static class Step1_Reducer extends Reducer<LongWritable,Text,Text,NullWritable>{
		
	}

	
	
}
