package com.tencent.mikemyzhao;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import com.tencent.mikemyzhao.Step1.Step1_Mapper.FileRecorder;

public class Step2 {
	/*
	 * 
	 * 处理Step1的数据 Step1输出的数据格式是：时间-上行，下行，总和 尝试计算每1分钟间隔的流量增长
	 * Mapper输出key为开始结束时间，value为该段时间内的增量
	 * 
	 * 
	 */
	public static boolean run(Configuration config, Map<String, String> paths) {

		try {
			FileSystem fs = FileSystem.get(config);
			Job job = Job.getInstance(config);

			job.setJobName("step2");

			job.setJarByClass(Step2.class);
			job.setMapperClass(Step2_Mapper.class);
			job.setReducerClass(Step2_Reducer.class);

			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);

			// 在Step2Input中指定输入文件
			FileInputFormat.addInputPath(job, new Path(paths.get("Step2Input")));

			// 在Step2Output中指定输出文件
			Path outpath = new Path(paths.get("Step2Output"));
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

	static class Step2_Mapper extends Mapper<LongWritable, Text,IntWritable,Text> {
		String starttime;
		String nowtime;
		String startdata;
		String nowdata; 
		int index;
		
		public int getIndex() {
			return index;
		}

		public void setIndex(int index) {
			this.index = index;
		}

		public String getStarttime() {
			return starttime;
		}

		public void setStarttime(String starttime) {
			this.starttime = starttime;
		}

		public String getNowtime() {
			return nowtime;
		}

		public void setNowtime(String nowtime) {
			this.nowtime = nowtime;
		}
		

		public String getStartdata() {
			return startdata;
		}

		public void setStartdata(String startdata) {
			this.startdata = startdata;
		}

		public String getNowdata() {
			return nowdata;
		}

		public void setNowdata(String nowdata) {
			this.nowdata = nowdata;
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// 对value进行处理
			String[] strs = StringUtils.split(value.toString(), '-');
			SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss.SSS");
			if (key.get() == 0) {
				this.setStarttime(strs[0]);
				this.setStartdata(strs[1]);
				System.out.println(starttime);
				this.setIndex(0);
			} else {
				String nt = strs[0];
				String nd = strs[1];
				String starttime = this.getStarttime();
				String startdata = this.getStartdata();
				try {
					Date dst = df.parse(this.getStarttime());
					Date dnt = df.parse(nt);
					long difftime = dnt.getTime() - dst.getTime();
					long days = difftime / (1000 * 60 * 60 * 24);
					long hours = (difftime - days * (1000 * 60 * 60 * 24)) / (1000 * 60 * 60);
					long minutes = (difftime - days * (1000 * 60 * 60 * 24) - hours * (1000 * 60 * 60)) / (1000 * 60);
					if (minutes < 2 && minutes >= 1) {
						System.out.println(this.getStarttime());						
						this.setStarttime(nt);
						this.setStartdata(nd);
						System.out.println(nt);
						System.out.println("" + days + "天" + hours + "小时" + minutes + "分");
						
						String[] strndata = StringUtils.split(nd.toString(),',');
						String nupdata = strndata[0];
						String ndowndata = strndata[1];
						String ntotdata = strndata[2];
						
						String[] strsdata = StringUtils.split(startdata.toString(),',');
						String supdata = strsdata[0];
						String sdowndata = strsdata[1];
						String stotdata = strsdata[2];
						
//						Double diffupdata = Double.parseDouble(String.format("%.3f",nupdata)) 
//								- Double.parseDouble(String.format("%.3f",supdata));
//						Double diffdowndata = Double.parseDouble(String.format("%.3f",ndowndata)) - 
//								Double.parseDouble(String.format("%.3f",sdowndata));
//						Double difftotdata = Double.parseDouble(String.format("%.3f",ntotdata)) 
//									- Double.parseDouble(String.format("%.3f",stotdata));
						//Double diffupdata = Double.parseDouble(String.format("%.3f",nupdata)) 
							//	- Double.parseDouble(String.format("%.3f",supdata));
						
						
						//Double diffdowndata = Double.parseDouble(String.format("%.3f",ndowndata)) - 
						//		Double.parseDouble(String.format("%.3f",sdowndata));
						Double difftotdata = Double.parseDouble(ntotdata)
								- Double.parseDouble(stotdata);
						BigDecimal b  = new   BigDecimal(difftotdata);  
						double f1 = b.setScale(3,BigDecimal.ROUND_HALF_UP).doubleValue();  
						
						String timev = new String(starttime + "-" + nt);
						String datav = new String(Double.toString(f1));
						//Text v = new Text(Double.toString(f1));
						
						//DoubleWritable v = new DoubleWritable(f1);
						this.index = index + 1;
						//System.out.println(index);
						IntWritable k = new IntWritable(this.index);
						Text v = new Text(timev + " "+datav);
						context.write(k,v);
						
					}else{
						//this.setStarttime(this.getStarttime());
						//this.setStartdata(this.getStartdata());
					}

				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

	}

	static class Step2_Reducer extends Reducer<IntWritable, Text, Text, NullWritable> {
		
//		@Override
//		protected void reduce(IntWritable key,Iterable<Text> i,Context context) 
//				throws IOException, InterruptedException{
//			
//			
//		}
			
	}
	
	

}
