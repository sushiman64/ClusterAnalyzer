package com.zikesjan.bigdata.analyze;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedHashMap;

import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class PointWritable implements Writable{

	private LinkedHashMap<String, MutableDouble> features;
	public double meanDistance;
	public long id;
	
	public PointWritable(){
		this.features = new LinkedHashMap<String, MutableDouble>();
	}
	

	public PointWritable(LinkedHashMap<String, MutableDouble> features) {
		super();
		this.features = features;
	}
	
	public LinkedHashMap<String, MutableDouble> getFeatures(){
		return this.features;
	}
	
	public void addFeature(String word, double tfidf){
		features.put(word, new MutableDouble(tfidf));
	}
	
	public void addFeatureFromString(String feature){
		String[] splited = feature.split(":");
		features.put(splited[0], new MutableDouble(Double.parseDouble(splited[1])));
	}
	
	public void addAllFeaturesFromString(String data){
		String[] features = data.split(" ");
		for(String feature : features){
			addFeatureFromString(feature);
		}
	}
		
	
	/**
	 * calculation of the cosine norm for the spherical clustering, vectors must be normalized before the call of this method
	 * @param other
	 * @return
	 */
	public double cosineNorm(PointWritable other){
		double result = 0.0;
		if(features.keySet().size() < other.features.keySet().size()){
			for(String s : features.keySet()){
				if(other.features.containsKey(s)){
					result += features.get(s).doubleValue() * other.features.get(s).doubleValue();
				}
			}
		}else{
			for(String s : other.features.keySet()){
				if(features.containsKey(s)){
					result += features.get(s).doubleValue() * other.features.get(s).doubleValue();
				}
			}
		}
		return result;
	}
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		Text t = new Text();
		t.readFields(in);
		features = new LinkedHashMap<String, MutableDouble>();
		addAllFeaturesFromString(t.toString());
		id = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		StringBuilder sb = new StringBuilder();
		for(String s : features.keySet()){
			sb.append(s+":"+features.get(s)+" ");
		}
		new Text(sb.toString()).write(out);
		out.writeLong(id);
	}


	public String getFeaturesString(){
		StringBuilder sb = new StringBuilder();
		for(String s : features.keySet()){
			sb.append(s+":"+features.get(s)+" ");
		}
		return sb.toString();
	}
	
	
	public LinkedHashMap<String, MutableDouble> getHashMap(){
		return this.features;
	}
	
	public void calculateMeanDistance(PointWritable mean){
		this.meanDistance = cosineNorm(mean);
	}


	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (id ^ (id >>> 32));
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PointWritable other = (PointWritable) obj;
		if (id != other.id)
			return false;
		return true;
	}


}
