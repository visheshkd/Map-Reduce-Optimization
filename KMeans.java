import java.io.*;
import java.net.URI;
import java.util.Hashtable;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;




class Point implements WritableComparable<Point> {
    public double x;
    public double y;

    public Point(){
        this.x=0.0;
        this.y=0.0;
    }
    public Point(double x1,double y1){
        this.x = x1;
        this.y = y1;
    }


    @Override
    public int compareTo(Point o) {
        if(Double.compare(this.x, o.x)==0)
            return (int) (this.y-o.y);
        else
            return (int) (this.x-o.x);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(x);
        dataOutput.writeDouble(y);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        x=dataInput.readDouble();
        y=dataInput.readDouble();

    }
    public String toString(){
        return Double.toString(x)+","+Double.toString(y);
    }
}
class Avg implements Writable{
    public double sumX;
    public double sumY;
    public long count;
    public Avg(){
        this.sumX=0.0;
        this.sumY=0.0;
        this.count=1;
    }
    public Avg(double sx, double sy, long ct){
        this.sumX=sx;
        this.sumY=sy;
        this.count=ct;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(sumX);
        dataOutput.writeDouble(sumY);
        dataOutput.writeLong(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        sumX=dataInput.readDouble();
        sumY=dataInput.readDouble();
        count=dataInput.readLong();
    }
    public String toString(){
        return Double.toString(sumX)+","+Double.toString(sumY)+","+Long.toString(count);
    }
}

public class KMeans {
    static Vector<Point> centroids = new Vector<Point>(100);
    static Hashtable<Point,Avg> table;


    public static class AvgMapper extends Mapper<Object,Text,Point,Avg> {
        public void setup(Context context) throws IOException {
            table=new Hashtable<Point, Avg>();
            URI[] paths = context.getCacheFiles();
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(paths[0]))));
            String s;
            while((s=reader.readLine()) != null){
                Point p = new Point(Double.parseDouble(s.split(",")[0]),Double.parseDouble(s.split(",")[1]));
                centroids.add(p);
            }
            reader.close();
            centroids.firstElement();
        }
        public void cleanup(Context context) throws IOException, InterruptedException {
            for(Point c:table.keySet()){        //for each key c in table
                context.write(c,table.get(c));
            }
        }
        public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
            centroids.firstElement();
            Scanner s1= new Scanner(value.toString()).useDelimiter(",");
            double eucld=0.0;
            double min_distance=15000.0; //random outlier value
            //initializing min_distance with large value , so that every euclidean distance(centroid-datapoint) is considered in if statement
            Point c = new Point();
            Point p1 = new Point(s1.nextDouble(),s1.nextDouble());
            for(Point p:centroids){
                //euclidean distance
                eucld = Math.sqrt(Math.pow(Math.abs(p.x-p1.x),2)+Math.pow(Math.abs(p.y-p1.y),2));
                if(eucld < min_distance){
                    min_distance = eucld;
                    c = p;
                }

            }

            if(table.get(c)==null)
                table.put(c,new Avg(p1.x,p1.y,1));
            else{
                table.put(c,new Avg(table.get(c).sumX+p1.x,table.get(c).sumY+p1.y,table.get(c).count+1));
            }
            //context.write(c,p1);
        }
    }

    public static class AvgReducer extends Reducer<Point,Avg,Point,Object> {
        public void reduce(Point key,Iterable<Avg> avgs, Context context) throws IOException, InterruptedException {
            double count =0;

            Point s = new Point();
            for(Avg a:avgs){
                count+=a.count;
                s.x += a.sumX;
                s.y += a.sumY;
            }
            s.x=s.x/count;
            s.y=s.y/count;
            context.write(s,null);
        }
    }

    public static void main ( String[] args ) throws Exception {
        // Create a new Job
        Configuration cong = new Configuration();
        Job job = Job.getInstance(cong);
        job.setJarByClass(KMeans.class);
        job.setJobName("KMeans");
        job.addCacheFile(new Path(args[1]).toUri());
        // Specify various job-specific parameters
        job.setMapOutputKeyClass(Point.class);
        job.setMapOutputValueClass(Avg.class);
        job.setMapperClass(AvgMapper.class);
        job.setReducerClass(AvgReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));

        job.waitForCompletion(true);




    }
}

