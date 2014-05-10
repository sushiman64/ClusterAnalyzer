package com.zikesjan.bigdata;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.zikesjan.bigdata.analyze.AnalyzatorMapper;
import com.zikesjan.bigdata.analyze.AnalyzatorReducer;
import com.zikesjan.bigdata.analyze.PointWritable;


public class AnalyzeMain {
	
    private static final String ACTUAL_MEANS = "/user/biadmin/output/means/part-r-00000";
    

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException, URISyntaxException {

		Path inputPath = new Path(args[2]);
		Path outputDir = new Path(args[3]);
		int kWords = Integer.parseInt(args[0]);
		int lDocuments = Integer.parseInt(args[1]);
		
		// Create configuration
		Configuration conf = new Configuration(true);
		conf.set("kWords", kWords+"");
		conf.set("lDocuments", lDocuments+"");
		Job analyze = new Job(conf, "Analyze");
		DistributedCache.addCacheFile(new URI(ACTUAL_MEANS), analyze.getConfiguration());
		analyze.setJarByClass(AnalyzatorMapper.class);
		analyze.setMapperClass(AnalyzatorMapper.class);
		analyze.setMapOutputKeyClass(IntWritable.class);
		analyze.setMapOutputValueClass(PointWritable.class);
		analyze.setReducerClass(AnalyzatorReducer.class);
		analyze.setOutputKeyClass(LongWritable.class);
		analyze.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(analyze, inputPath);
		analyze.setInputFormatClass(KeyValueTextInputFormat.class);
		FileOutputFormat.setOutputPath(analyze, outputDir);
		MultipleOutputs.addNamedOutput(analyze, "words", TextOutputFormat.class, LongWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(analyze, "documents", TextOutputFormat.class, LongWritable.class, Text.class);

		
		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		// Execute job
		int code = analyze.waitForCompletion(true) ? 0 : 1;
				
		System.exit(code);
	}
}
