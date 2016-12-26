package edu.indiana.d2i.cassandra;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.List;

import edu.indiana.d2i.nosql.Retriever;
import edu.indiana.d2i.tools.Configuration;
import edu.indiana.d2i.tools.Tools;

public class PerfTestDriver2 {


	
	public static void main(String[] args) throws InterruptedException, FileNotFoundException {
		
		PrintWriter pw = null;
		//if(Configuration.getProperty("ACCESS_PATTERN").equals("VOLUME")) {
			pw = new PrintWriter(new FileOutputStream("overall-stats-2.txt", true));
	//	} else {
	//		pw = new PrintWriter(new FileOutputStream("overall-page-stats.txt", true));
	//	}
		
		StringBuilder fieldsBuilder = new StringBuilder();
		fieldsBuilder.append("time").append("\t").append("volumePerSec").append("\t").append("bytesPerSec").append('\t').append("threads");
		pw.println(fieldsBuilder.toString());
		
		int threadNum = Integer.parseInt(Configuration.getProperty("THREAD_NUM"));
		
		Thread [] workloadThreads = new Thread[threadNum];
		
		for(int i=0; i<threadNum; i++) {
			
			Retriever retriever = null;
		//	if(Configuration.getProperty("ACCESS_PATTERN").equals("VOLUME")) {
				retriever = new CassandraVolumeRetriever2(i);
	//		} else {
	//			retriever = new CassandraPageRetriever(i);
	//		}
			
			Tools.generateRandomInputVolumeList(new File("all.txt"), Integer.parseInt(Configuration.getProperty("INPUT_SIZE")));
			List<String> volumesToRetrieve = Tools.getVolumeIds(new File(Configuration.getProperty("VOLUME_ID_LIST")));
			CassandraWorkloadPerformer workloadPerformer = new CassandraWorkloadPerformer(retriever, volumesToRetrieve);
			workloadThreads[i] = workloadPerformer;
		}
		
		for(Thread thread: workloadThreads) {
			thread.start();
		}
		
		boolean running = true;
		
		long timeIntervalInSec = 4;
		int i=0;
		while(running) {
			
			Thread.sleep(timeIntervalInSec*1000);
			boolean flag = false;
			for (Thread t : workloadThreads) {
				flag = (t.isAlive() || flag);
			}
			running = running && flag;
			
			double bytesPerSec = Retriever.getByteCount().doubleValue() / (double) (timeIntervalInSec);
			Retriever.reSetByteCount();
			double volumePerSec = Retriever.getVolumeCount().doubleValue() / (double) (timeIntervalInSec);
			Retriever.reSetVolumeCount();
		//	double pagePerSec = Retriever.getPageCount().doubleValue() / (double) (timeIntervalInSec);
		//	Retriever.reSetPageCount();
			i++;
		
			pw.println(i*timeIntervalInSec + "\t" + volumePerSec + "\t" + bytesPerSec + "\t" + threadNum); pw.flush();
		}
		
		/*workloadPerformer.start();
		try {
			workloadPerformer.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}*/
		pw.flush();pw.close();
		CassandraManager.shutdown();
	
	}
	
	
	public static class CassandraWorkloadPerformer extends Thread {

		private Retriever retriever;
		private List<String> volumesToRetrieve;
		
		public CassandraWorkloadPerformer(Retriever retriever, List<String> volumesToRetrieve) {
			this.retriever = retriever;
			this.volumesToRetrieve = volumesToRetrieve;
		}
		
		public void run() {
			retriever.retrieve(volumesToRetrieve);
			retriever.releaseResource();
		}
	}

}
