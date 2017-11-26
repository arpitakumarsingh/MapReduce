package org.hadoop.trainings;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
@Override
protected void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
	long sum = 0;
	for(LongWritable value: values) {
		sum = sum +value.get();
	}
	
	// assign the sum to the corresponding word or key
	context.write(key , new LongWritable(sum));
}
}

