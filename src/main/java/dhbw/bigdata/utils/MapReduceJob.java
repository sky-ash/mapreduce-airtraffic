package dhbw.bigdata.utils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import dhbw.bigdata.Main;

public class MapReduceJob {
    private final String jobName;
    private final Class<? extends Mapper> mapperClass;
    private final Class<? extends Reducer> reducerClass;
    private final Class<? extends Reducer> combinerClass;
    private final Class<?> outputKeyClass;
    private final Class<?> outputValueClass;
    private final Class<? extends InputFormat> inputFormatClass;
    private final Class<? extends OutputFormat> outputFormatClass;
    private String inputPath;
    private String outputPath;

    public MapReduceJob(String jobName,
                        Class<? extends Mapper> mapperClass,
                        Class<? extends Reducer> reducerClass,
                        Class<? extends Reducer> combinerClass,
                        Class<?> outputKeyClass,
                        Class<?> outputValueClass,
                        Class<? extends InputFormat> inputFormatClass,
                        Class<? extends OutputFormat> outputFormatClass) {
        this.jobName = jobName;
        this.mapperClass = mapperClass;
        this.reducerClass = reducerClass;
        this.combinerClass = combinerClass;
        this.outputKeyClass = outputKeyClass;
        this.outputValueClass = outputValueClass;
        this.inputFormatClass = inputFormatClass;
        this.outputFormatClass = outputFormatClass;
        setDefaultPaths(jobName);
    }

    public static final String INPUT_CSV = "/input/airport_traffic.csv";
    public static final String PREPROCESSING_OUTPUT = "/output/preprocessing/";
    public static final String TEMP_OUTPUT = "/output/temp/";
    public static final String COUNTRY_RESULTS = "/output/countries/";
    public static final String AIRPORT_RESULTS = "/output/airports/";

    private void setDefaultPaths(String jobName) {
        if (jobName.toLowerCase().contains("preprocessing")) {
            setPaths(INPUT_CSV, PREPROCESSING_OUTPUT);
        } else if (jobName.toLowerCase().contains("airport")) {
            setPaths(PREPROCESSING_OUTPUT, AIRPORT_RESULTS);
        } else if ((jobName.toLowerCase().contains("country")) || (jobName.toLowerCase().contains("countries"))) {
            if (jobName.toLowerCase().contains("calc")) {
                setPaths(PREPROCESSING_OUTPUT, TEMP_OUTPUT);
            } else if (jobName.toLowerCase().contains("sort")) {
                setPaths(TEMP_OUTPUT, COUNTRY_RESULTS);
            }
        }
    }

    public void setPaths(String inputPath, String outputPath) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public String getInputPath() {
        return inputPath;
    }

    public String getOutputPath() {
        return outputPath;
    }

    
    public void run() throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(Main.class);
        job.setMapperClass(mapperClass);
        job.setReducerClass(reducerClass);
        if (combinerClass != null) job.setCombinerClass(combinerClass);
        job.setOutputKeyClass(outputKeyClass);
        job.setOutputValueClass(outputValueClass);
        job.setInputFormatClass(inputFormatClass);
        job.setOutputFormatClass(outputFormatClass);
        
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        if (!job.waitForCompletion(true)) {
            throw new RuntimeException("Job " + jobName + " failed.");
        }
    }
}
