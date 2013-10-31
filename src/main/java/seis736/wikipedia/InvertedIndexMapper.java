package seis736.wikipedia;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import edu.umd.cloud9.collection.wikipedia.language.EnglishWikipediaPage;

/*
 * Input format: sequence file
 * Input key: docno (Sequentially-Numbered)
 * Input value: EnglishWikipediaPage
 */
public class InvertedIndexMapper extends Mapper<IntWritable, EnglishWikipediaPage, Text, TfWritable> {
	
	public static enum ARTICLE_COUNTER {
		Article_Counter,
	}
	
	private Text word = new Text();
	private TfWritable tfwritable = new TfWritable();
	
	@Override
	public void map(IntWritable key, EnglishWikipediaPage value, Context context) 
			throws IOException, InterruptedException {
		
		Map<String, Integer> wordcount = new HashMap<String, Integer>();
		if (value.isArticle()) {
			// Record the number of articles
			context.getCounter(ARTICLE_COUNTER.Article_Counter).increment(1);
			
			String[] words = value.getContent().toString().split("\\W+");
			for (String word : words) {
				if (!wordcount.containsKey(word)) {
					wordcount.put(word, 1);
				} else {
					wordcount.put(word, wordcount.get(word) + 1);
				}
			}
			int max_count = Collections.max(wordcount.values());
			
			for (String word : wordcount.keySet()) {
				this.word.set(word);
				tfwritable.setDocno(key.get());
				tfwritable.setTf((float)wordcount.get(word) / max_count);
				context.write(this.word, this.tfwritable);
			}
		}
	}
}
