import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class PrepareDataPR extends Configured implements Tool {
    static public void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new PrepareDataPR(), args);
        System.exit(ret);
    }

    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());

        job.setJarByClass(PrepareDataPR.class);
        job.setJobName(PrepareDataPR.class.getCanonicalName());

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(PrepMapper.class);
        job.setReducerClass(PrepReducer.class);
        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getJobConf(args[0], args[1]+"/iter0");
        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static class PrepMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Long from = Long.valueOf(value.toString().split("\t")[0]);
            Long to = Long.valueOf(value.toString().split("\t")[1]);
            context.write(new LongWritable(from), new LongWritable(to));
        }
    }

    public static class PrepReducer extends Reducer<LongWritable, LongWritable, LongWritable, Text> {
        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> nums, Context context) throws IOException, InterruptedException {
            StringBuilder links = new StringBuilder();
            links.append(10000. / 564549.); //or 572418
            links.append("\t");
            String prefix = "";
            for (LongWritable val : nums) {
                links.append(prefix);
                prefix = ",";
                links.append(val);
            }
            context.write(key, new Text(links.toString()));
        }
    }
}


