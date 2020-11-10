import java.io.IOException;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        
        
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
        	
            StringTokenizer itr = new StringTokenizer(value.toString());
            
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
            
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
    	
    	private TreeMap<Integer, Text> tmap;  
    	
    	@Override
        public void setup(Context context) throws IOException, 
                                         InterruptedException 
        { 
    		tmap = new TreeMap<Integer, Text>(); 
        } 
    	
//        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
        	
            int sum = 0;
            
            for (IntWritable val : values) {
                sum += val.get();
            }
            
            System.out.print(key + " " + sum);
            
            if (tmap.size() < 5) {
            	// simply add
            	tmap.put((Integer)sum, key);
            	System.out.print("put");
            	
            } else {
            	if (sum < tmap.lastKey()) {
            		tmap.put((Integer)sum, key);
            		tmap.remove(tmap.lastKey());
            		
            		System.out.print("add");
            	} else {
            		System.out.print("ignore");
            	}
            }
            
//            result.set(sum);
//            
//            context.write(key, result);
            
        }
        
        @Override
        public void cleanup(Context context) throws IOException, 
                                           InterruptedException 
        { 
            for (Entry<Integer, Text> entry : tmap.entrySet())  
            { 
      
            	Integer count = entry.getKey(); 
            	Text name = entry.getValue();  
                context.write(new Text(name), new IntWritable(count)); 
            } 
        } 
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (job.waitForCompletion(true)) {
            long endTime = System.currentTimeMillis();
            System.out.println("Execution time: " + (endTime-startTime) + " ms");
            System.exit(0);
        } else {
            System.exit(1);
        }
        
        
    }
}
