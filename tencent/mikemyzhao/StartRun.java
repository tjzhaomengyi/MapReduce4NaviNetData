package com.tencent.mikemyzhao;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/*
 * 启动整个作业
 * 
 */
public class StartRun {
	public static void main(String[] args){
		Configuration config = new Configuration();
		config.set("fs.defaultFS","hdfs://node001:9000");
		config.set("yarn.resoucemanager.hostname","node001");
		
		//把mr工作的输入输出目录放在map中
		Map<String,String> paths = new HashMap<String,String>();
		String dataloc = "/MapSDKPerRes/20170428/Para/navi_4月26日5时36分_4月26日5时57分/Device Network_04-26 05-57-09.csv";
		String dataver = StringUtils.split(dataloc,"/")[1];
		String datatime = StringUtils.split(dataloc, "/")[3];
		String outputloc = "/MapSDKPerRes/perstepfiles/"+dataver+"/"+datatime+"/";
		paths.put("Step1Input", dataloc);
		paths.put("Step1Output",outputloc+"step1");
		
		String step1output = outputloc+"step1";
		paths.put("Step2Input", step1output);
		paths.put("Step2Output", outputloc+"step2");
		
		String step2output = outputloc + "step2";
		paths.put("Step3Input", paths.get("Step2Output"));
		paths.put("Step3Output",outputloc+"step3");
		
		Step1.run(config,paths);
		Step2.run(config, paths);
		Step3.run(config, paths);
	}
}
