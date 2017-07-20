package com.tencent.mikemyzhao;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Step3 {
//使用map计算均值
	
	public static boolean run(Configuration config, Map<String, String> paths) {

		try {
			FileSystem fs = FileSystem.get(config);
			Job job = Job.getInstance(config);

			job.setJobName("step3");

			job.setJarByClass(Step3.class);
			job.setMapperClass(Step3_Mapper.class);
			job.setReducerClass(Step3_Reducer.class);

			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);

			// 在Step2Input中指定输入文件
			FileInputFormat.addInputPath(job, new Path(paths.get("Step3Input")));

			// 在Step2Output中指定输出文件
			Path outpath = new Path(paths.get("Step3Output"));
			if (fs.exists(outpath)) {
				fs.delete(outpath, true);
			}
			FileOutputFormat.setOutputPath(job, outpath);

			boolean f = job.waitForCompletion(true);
			return f;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	
	static class Step3_Mapper extends Mapper<LongWritable, Text,IntWritable,Text> {
		@Override 
		protected void map(LongWritable key,Text text,Context context) 
				throws IOException, InterruptedException{
			
			IntWritable k = new IntWritable((int) 1);
			//System.out.println(text.toString());
			context.write(k, text);
		}
	
	}
	
	static class Step3_Reducer extends Reducer<IntWritable, Text, Text, NullWritable> {
	
		@Override
		protected void reduce(IntWritable key,Iterable<Text> i,Context context) 
				throws IOException, InterruptedException{
			double sum1 = 0;
			double sum2 = 0;
			int cnt = 0;
			double data = 0;
			//System.out.println(key.toString());
			for (Text value : i){
				
				String[] rs = value.toString().split(" ");
				data = Double.parseDouble(rs[1]);
				System.out.println("this data :" + Double.toString(data));
				sum1 = sum1 + data;
				sum2 = setDouble3Pos(sum2 + Math.pow(data, 2));
				cnt = cnt + 1;
			}
			System.out.println("this sum2 :" + Double.toString(sum2));
			System.out.println("this sum1 :" + Double.toString(sum1));
			double mean = sum1 / cnt;
			BigDecimal b  = new   BigDecimal(mean);  
			mean = b.setScale(3,BigDecimal.ROUND_HALF_UP).doubleValue();
			
			//double var = setDouble3Pos(sum2 / cnt) - setDouble3Pos(Math.pow(mean,2));
			double var =setDouble3Pos(setDouble3Pos(sum2 / cnt) - setDouble3Pos(2*sum1*mean/cnt) + setDouble3Pos(sum2/cnt));
			
			StringBuffer sb = new StringBuffer();
			sb.append("mean:" + Double.toString(mean) + " var:"+Double.toString(var));
			context.write(new Text(sb.toString()), NullWritable.get());
			
		}
		
	}
	
	protected static double setDouble3Pos(double b){
		BigDecimal tmp = new BigDecimal(b);
		Double res = tmp.setScale(3,BigDecimal.ROUND_HALF_UP).doubleValue();
		return res;
	}
	
}
