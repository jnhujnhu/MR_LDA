package utils;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;


/**
 * Created by Kevin on 11/12/2016.
 */
public class DocRecordReader extends RecordReader<LongWritable, Text> {

    private LineRecordReader lineRecordReader = null;
    private LongWritable key = null;
    private Text value = null;

    @Override
    public void close() throws IOException {
        if (null != lineRecordReader) {
            lineRecordReader.close();
            lineRecordReader = null;
        }
        key = null;
        value = null;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return lineRecordReader.getProgress();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        close();

        lineRecordReader = new LineRecordReader();
        lineRecordReader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!lineRecordReader.nextKeyValue()) {
            key = null;
            value = null;
            return false;
        }

        // This is where you should implement your custom logic.
        Text line = lineRecordReader.getCurrentValue();
        String str = line.toString();
        int blank_pos = str.indexOf(" ");
        Long doc_num = Long.parseLong(str.substring(0, blank_pos));

        key = new LongWritable(doc_num);
        value = new Text(str.substring(blank_pos + 1));

        return true;
    }
}
