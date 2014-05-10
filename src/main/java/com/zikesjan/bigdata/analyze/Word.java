package com.zikesjan.bigdata.analyze;

public class Word implements Comparable<Word>{

	public double value;
	public String word;
	
	
	public Word(double value, String word) {
		super();
		this.value = value;
		this.word = word;
	}



	@Override
	public int compareTo(Word o) {
		return this.value > o.value ? 1:-1;
	}

}
