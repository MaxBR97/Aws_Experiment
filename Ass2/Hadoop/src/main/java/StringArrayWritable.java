import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StringArrayWritable extends ArrayWritable implements WritableComparable {
    public StringArrayWritable() {
        super(Text.class);
    }
    public StringArrayWritable(Text[] values){
        super(Text.class, values);
        Text[] texts = new Text[values.length];
        for (int i = 0; i < values.length; i++) {
            texts[i] = new Text(values[i]);
        }
        set(texts);
    }
    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        for(String s : super.toStrings()){
            sb.append(s).append("\t");
        }
        return sb.toString();
    }
        @Override
        public int compareTo(Object arr) {
        return 0;
        }

        public int hashCode(){
            int ans = 0;
            for(Writable t : this.get()){
                ans+=t.hashCode();
            }
            return ans;
        }
    }