
题目2：编写MapReduce，统计`/user/hadoop/mapred_dev/ip_time` 中去重后的IP数，越节省性能越好。（35分）

---
package ks;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class QSTMapReduce1 {

  public static class MyMapper 
       extends Mapper<Object, Text, Text, Text>{

	  public void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String line =value.toString();
			String str = line.split("\t")[0];
			context.write(new Text(str), new Text("a"));
  }

  public static class MyReducer 
       extends Reducer<Text,Text,Text,Text> {

	  public void reduce(Text key, Iterable<Text> values, Context context                       ) throws IOException, InterruptedException {      
      context.write(key, new Text("on"));
    }
  }

  	public static void main(String[] args) throws Exception {
  		Configuration conf = new Configuration();
  		

    	Job job = new Job(conf, "sum");
    	job.setJarByClass(QSTMapReduce1.class);
    	job.setMapperClass(MyMapper.class);
    	job.setCombinerClass(MyReducer.class);
    	job.setReducerClass(MyReducer.class);
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(Text.class);
    	job.setNumReduceTasks(1);
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	job.waitForCompletion(true);
      Long linenumber=job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter","REDUCE_OUTPUT_RECORDS").getValue();
      System.out.println("ip数："+linenumber);
        
  		}	
  	}
}


运行完之后，描述程序里所做的优化点，每点+5分。
