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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class PageRank extends Configured implements Tool {

    class Pair {
        final long a;
        final Double b;

        public Pair(final long a, final Double b) {
            this.a = a;
            this.b = b;
        }
    }

    private ArrayList<Long> top_30(final FileSystem fs, final String filename) throws IOException {
        final ArrayList<Pair> tmp = new ArrayList<Pair>();
        final Path pt = new Path(filename);
        final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            final String[] split = line.split("\t");
            final Double s = Double.parseDouble(split[1]);
            tmp.add(new Pair(Long.parseLong(split[0]), s));
        }
        fs.close();
        final ArrayList<Long> top = new ArrayList<>();
        Collections.sort(tmp, new Comparator<Pair>() {
            public int compare(final Pair l, final Pair r) {
                return -Double.compare(l.b, r.b);
            }
        });
        final String fname = "PR/result.txt";
        final Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fname), "utf-8"));
        for (int i = 0; i < 30; ++i) {
            top.add(tmp.get(i).a);
            writer.write(String.valueOf(tmp.get(i).a) + "\t" + String.valueOf(tmp.get(i).b)+"\n");
        }
        writer.close();
        return top;
    }

    @Override
    public int run(final String[] args) throws Exception {

        final FileSystem fs = FileSystem.get(getConf());
        // final PageRank pagerank = new PageRank();
        ArrayList<Long> old_top = new ArrayList<>(30);
        int res = 0;
        for (int i = 0; i < 10; ++i) {

            if (fs.exists(new Path(args[0] + "/iter" + String.valueOf(i)))) {
                final String fname = args[0] + "/iter" + String.valueOf(i) + "/part-r-00000";
                old_top = top_30(fs, fname);
                continue;
            }
            final String inPath = args[0] + "/iter" + String.valueOf(i - 1);
            final String lastOutPath = args[0] + "/iter" + String.valueOf(i);
            final Job job = job(inPath, lastOutPath);
            res += job.waitForCompletion(true) ? 0 : 1;
            final String fname = args[0] + "/iter" + String.valueOf(i) + "/part-r-00000";
            final ArrayList<Long> new_top = top_30(fs, fname);
            if (old_top.containsAll(new_top))
                break;
            old_top = new_top;
        }
        return res;
    }

    static public void main(final String[] args) throws Exception {
        final int ret = ToolRunner.run(new PageRank(), args);
        System.exit(ret);
    }

    private Job job(final String in, final String out)
            throws IOException, ClassNotFoundException, InterruptedException {

        final Job job = Job.getInstance(getConf());
        job.setJarByClass(PageRank.class);

        FileInputFormat.setInputPaths(job, new Path(in));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(PageRankMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(PageRankJob2Reducer.class);

        return job;

    }

    public static class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(final LongWritable key, final Text value, final Context context)
                throws IOException, InterruptedException {

            final int ind1 = value.find("\t");
            final int ind2 = value.find("\t", ind1 + 1);

            final String page = Text.decode(value.getBytes(), 0, ind1);
            final String pageRank = Text.decode(value.getBytes(), ind1 + 1, ind2 - (ind1 + 1));
            final String links = Text.decode(value.getBytes(), ind2 + 1, value.getLength() - (ind2 + 1));

            final String[] allOtherPages = links.split(",");
            for (final String otherPage : allOtherPages) {
                if (otherPage.equals(""))
                    continue;
                final Text pageRankWithTotalLinks = new Text(pageRank + "\t" + allOtherPages.length);
                context.write(new Text(otherPage), pageRankWithTotalLinks);
            }

            context.write(new Text(page), new Text("|" + links));

        }

    }

    public static class PageRankJob2Reducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context)
                throws IOException, InterruptedException {

            final StringBuilder links = new StringBuilder();
            double new_rank = 0.0;

            for (final Text value : values) {
                final String content = value.toString();
                if (content.startsWith("|")) {
                    links.append(content.substring(1));
                } else {
                    final String[] split = content.split("\t");
                    final double pageRank = Double.parseDouble(split[0]);
                    final int totalLinks = Integer.parseInt(split[1]);
                    new_rank += (pageRank / totalLinks);
                }

            }
            final Double alpha = 0.85;
            new_rank = alpha * new_rank + (1 - alpha) * (10000. / 564549.); //or 572418
            context.write(key, new Text(new_rank + "\t" + links));

        }

    }
}