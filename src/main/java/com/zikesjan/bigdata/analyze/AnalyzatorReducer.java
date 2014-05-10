package com.zikesjan.bigdata.analyze;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.zikesjan.bigdata.analyze.helpers.Document;
import com.zikesjan.bigdata.analyze.helpers.Word;


public class AnalyzatorReducer extends
		Reducer<IntWritable, PointWritable, Text, Text> {

	private List<PointWritable> means;
	private MultipleOutputs<Text, Text> mos;
	private int kWords;
	private int lDocuments;
	
	/**
	 * reading the data from the distributed cache
	 */
	public void setup(Context context) throws IOException, InterruptedException {
		means = new ArrayList<PointWritable>();
		Path[] localCacheFiles = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());
		BufferedReader br = new BufferedReader(new FileReader(
				localCacheFiles[0].toString()));
		String lineString = br.readLine();
		while (lineString != null) {
			String[] keyValue = lineString.split("\t");
			PointWritable mwc = new PointWritable();
			mwc.addAllFeaturesFromString(keyValue[1]);
			means.add(mwc);
			lineString = br.readLine();
		}
		br.close();
		
		//setting up multiple outputs environment
		mos = new MultipleOutputs<Text, Text>(context);
		
		//getting parameters from the main class
		Configuration conf = context.getConfiguration();
		kWords = Integer.parseInt(conf.get("kWords"));
		lDocuments = Integer.parseInt(conf.get("lDocuments"));
		
	}

	public void reduce(IntWritable key, Iterable<PointWritable> values,
			Context context) throws IOException, InterruptedException {
		
		//Getting the k most important words for the given cluster in O(nlog(k))
		LinkedHashMap<String, MutableDouble> meanMap = means.get(key.get()).getHashMap();
		PriorityQueue<Word> wordPQ = new PriorityQueue<Word>(kWords);
		int counter = 0;
		for(String s : meanMap.keySet()){
			if(counter >= kWords){
				if(meanMap.get(s).doubleValue() > wordPQ.peek().value){
					wordPQ.poll();
					wordPQ.add(new Word(meanMap.get(s).doubleValue(), s));
				}
			}else{
				wordPQ.add(new Word(meanMap.get(s).doubleValue(), s));
			}
			counter++;
		}
		StringBuilder sb = new StringBuilder();
		while(!wordPQ.isEmpty()){
			Word current = wordPQ.poll();
			sb.append(current.word+":"+current.value+", ");
		}
		mos.write("words", key, new Text(sb.toString()));
		
		
		//getting the l most similar documents to the mean in O(nlog(l))
		PriorityQueue<Document> documentPQ = new PriorityQueue<Document>();
		counter = 0;
		for (PointWritable mwc : values) {
			mwc.calculateMeanDistance(means.get(key.get()));
			Document actual = new Document(mwc.getFeaturesString(), mwc.id, mwc.meanDistance);
			if(counter >= lDocuments){
				if(actual.compareTo(documentPQ.peek()) < 0){
					documentPQ.poll();
					documentPQ.add(actual);
				}
			}else{
				documentPQ.add(actual);
			}
			counter++;
		}
		while(!documentPQ.isEmpty()){
			Document current = documentPQ.poll();
			mos.write("documents", key, new Text(current.meanDistance+":>> "+current.id+": "+current.features));
		}
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException {
		 mos.close();
	}
}
