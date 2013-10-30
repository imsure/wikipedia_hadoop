package seis736.wikipedia;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;

import edu.umd.cloud9.collection.wikipedia.*;

public class WikipediaDriver extends Configured implements Tool {
	
	private SequenceFile.Reader reader;
	
	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		
		job.setJarByClass(this.getClass());
		job.setJobName("Wikipedia: convert sequence files text files");
		
		FileInputFormat.addInputPath(job, new Path("enwiki.block/part-m-00000"));
		FileOutputFormat.setOutputPath(job, new Path("enwiki.titles"));
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(SeqFile2TextFileMapper.class);
		job.setNumReduceTasks(0); // A mapper only job
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		return job.waitForCompletion(true) ? 0 : -1;
	}
	
	/*
	 * Demo for reading sequence files.
	 */
	public void readSeqFile() throws IOException {
		String uri = "enwiki.block/part-m-00325";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);
				
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			long position = reader.getPosition();
			
			// Iterate through the records of sequence file
			while (reader.next(key, value)) {
				String syncSeen = reader.syncSeen() ? "*" : "";
				System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key.getClass(), value.getClass());
				position = reader.getPosition();
			}

		} finally {
			IOUtils.closeStream(reader);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new WikipediaDriver(), args);
		System.exit(res);
	}
}
