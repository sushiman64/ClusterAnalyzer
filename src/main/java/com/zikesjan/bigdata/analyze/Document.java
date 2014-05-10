package com.zikesjan.bigdata.analyze;

public class Document implements Comparable<Document>{
	
	public String features;
	public long id;
	public double meanDistance;
	
	public Document(String features, long id, double meanDistance) {
		super();
		this.features = features;
		this.id = id;
		this.meanDistance = meanDistance;
	}

	
	@Override
	public int compareTo(Document o) {
		return this.meanDistance > o.meanDistance ? 1:-1;
	}
	
	
	
}
