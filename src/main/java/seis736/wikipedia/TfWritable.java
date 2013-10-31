package seis736.wikipedia;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;

import java.io.*;

/*
 * A custom writable for <document number, term frequency> pair
 */
public class TfWritable implements WritableComparable<TfWritable> {

	private int docno;
	private float tf;
	
	public TfWritable() {
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(docno);
		out.writeFloat(tf);
	}
	
	public void readFields(DataInput in) throws IOException {
		docno = in.readInt();
		tf = in.readFloat();
	}
	
	public void setDocno(int docno) {
		this.docno = docno;
	}
	
	public int getDocno () {
		return this.docno;
	}
	
	public void setTf(float tf) {
		this.tf = tf;
	}
	
	public float getTf () {
		return this.tf;
	}
	
	@Override
	public String toString() {
		return this.docno + "\t" + this.tf;
	}
	
	@Override
	public int hashCode() {
	    final int prime = 31;
	    int result = 1;
	    result = prime * result + docno;
	    result = prime * result + Float.floatToIntBits(tf);
	    
	    return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (this.getClass() != obj.getClass()) return false;
		
		TfWritable other = (TfWritable) obj;
		if (this.docno != other.docno) {
			return false;
		} else {
			return true;
		}
	}
	
	public int compareTo(TfWritable other) {
		return (this.docno < other.docno) ? -1 : (this.docno == other.docno) ? 0 : 1;
	}
}
