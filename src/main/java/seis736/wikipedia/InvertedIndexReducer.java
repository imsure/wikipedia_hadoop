package seis736.wikipedia;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class InvertedIndexReducer extends Reducer<Text, TfWritable, IdfWritable, TfArrayWritable> {

	private IdfWritable idfwritable = new IdfWritable();
	private TfArrayWritable tfarray = new TfArrayWritable();
	
	@Override
	public void reduce(Text key, Iterable<TfWritable> values, Context context) 
			throws IOException, InterruptedException {
	
		ArrayList<TfWritable> tf_list = new ArrayList<TfWritable>();
		for (TfWritable tf : values) {
			tf_list.add(tf);
		}
		
		long num_of_articles = context.getCounter(InvertedIndexMapper.ARTICLE_COUNTER.Article_Counter).getValue();
		double idf = Math.log((double)num_of_articles / tf_list.size()) / Math.log(2);
		
		idfwritable.setWord(key.toString());
		idfwritable.setIdf((float)idf);
		
		tfarray.set((TfWritable[])tf_list.toArray());
		context.write(idfwritable, tfarray);
	}
}
