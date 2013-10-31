package seis736.wikipedia;

import org.apache.hadoop.io.ArrayWritable;

public class TfArrayWritable extends ArrayWritable {
	public TfArrayWritable() {
		super(TfWritable.class);
	}
}
