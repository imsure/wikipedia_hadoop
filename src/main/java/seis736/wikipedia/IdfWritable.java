package seis736.wikipedia;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/*
 * A custom writable for <word, inverted document frequency> pairs.
 */
public class IdfWritable implements WritableComparable<IdfWritable> {

	private String word;
	private float idf;
	
	public IdfWritable() {
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeChars(word);
		out.writeFloat(idf);
	}
	
	public void readFields(DataInput in) throws IOException {
		word = in.readUTF();
		idf = in.readFloat();
	}
	
	public void setWord(String word) {
		this.word = word;
	}
	
	public String getDocno () {
		return this.word;
	}
	
	public void setIdf(float idf) {
		this.idf = idf;
	}
	
	public float getIdf () {
		return this.idf;
	}
	
	@Override
	public String toString() {
		return this.word + "\t" + this.idf;
	}
	
	@Override
	public int hashCode() {
	    final int prime = 31;
	    int result = 1;
	    result = prime * result + ((word == null) ? 0 : word.hashCode());
	    result = prime * result + Float.floatToIntBits(idf);
	    
	    return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (this.getClass() != obj.getClass()) return false;
		
		IdfWritable other = (IdfWritable) obj;
		return word.equals(other.word);
	}
	
	public int compareTo(IdfWritable other) {
		return word.compareTo(other.word);
	}
}
