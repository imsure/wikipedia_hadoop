package seis736.wikipedia;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.*;

import edu.umd.cloud9.collection.wikipedia.*;
import edu.umd.cloud9.collection.wikipedia.language.EnglishWikipediaPage;

/**
 * The Mapper takes Wikipedia pages stored as sequence files as input.
 *
 */
public class SeqFile2TextFileMapper extends Mapper<IntWritable, EnglishWikipediaPage, IntWritable, Text> {
	
	//private IntWritable docno = new IntWritable(); // key for input sequence file
	//private WikipediaPage page = new EnglishWikipediaPage(); // value for input sequence file
	private Text title = new Text();
	
	@Override
	public void map(IntWritable key, EnglishWikipediaPage value, Context context) 
			throws IOException, InterruptedException {
		title.set(value.getDocid());
		context.write(key, title);
	}
}
