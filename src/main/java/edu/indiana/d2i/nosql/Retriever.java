package edu.indiana.d2i.nosql;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public abstract class Retriever {
	
	protected static AtomicLong volumeCount = new AtomicLong(0);
	protected static AtomicLong overallVolumeCount = new AtomicLong(0);
	protected static AtomicLong pageCount = new AtomicLong(0);
	protected static AtomicLong overallPageCount = new AtomicLong(0);
	protected static AtomicLong byteCount = new AtomicLong(0);
	protected static AtomicLong overallByteCount = new AtomicLong(0);
	
	public abstract void retrieve(List<String> volumesToRetrieve);
	public abstract void releaseResource();
	
	public static AtomicLong getVolumeCount() {
		return volumeCount;
	}
	public static void reSetVolumeCount() {
		volumeCount.set(0);;
	}
	public static AtomicLong getPageCount() {
		return pageCount;
	}
	public static void reSetPageCount() {
		pageCount.set(0);
	}
	public static AtomicLong getByteCount() {
		return byteCount;
	}
	public static void reSetByteCount() {
		byteCount.set(0);;
	}
	
}
