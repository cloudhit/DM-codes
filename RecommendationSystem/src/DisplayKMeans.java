import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.clustering.display.DisplayClustering;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.ManhattanDistanceMeasure;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.utils.clustering.ClusterDumper;

public class DisplayKMeans extends DisplayClustering
{
	private static final long serialVersionUID = 1L;
	
	public static void main(String[] args) throws Exception
	{
		DistanceMeasure measure = new ManhattanDistanceMeasure();
  
	    Path output = new Path("exp/kmean");
	    Path output1 = new Path("exp/kmean1");
	    Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(conf);
	    IntWritable key = new IntWritable();
	    VectorWritable value = new VectorWritable(); 
	  
	    SequenceFile.Writer writer = new SequencieFile.Writer(fs, conf,
	        output, key.getClass(), VectorWritable.class);
	    BufferedReader br = new BufferedReader(new FileReader("dataForKMeans"));
	    String line;
	    int counter=0;
	    double[][] data = new double[Reformat.userNum+1][Reformat.k];
	 
	    while ((line = br.readLine()) != null)
	    {
	       counter++;
		    Pattern DELIMITER = Pattern.compile("[\t,]");
			String k[]= DELIMITER.split(line); 
	
			   
			   
			int keyid=Integer.parseInt(k[0]);
			key.set(keyid);
			for (int i=0;i<k.length-1;i++)
			{
				double item =Double.parseDouble(k[i+1]); 
				data[keyid][i]=item;
			} 
			Vector v= new RandomAccessSparseVector(data[keyid].length);
			v.assign(data[keyid]);
			value.set(v);
			writer.append(key, value);
	    }

	    System.out.println("counter:"+counter);
	    writer.close();
	    RandomUtils.useTestSeed();
	  
	    boolean runClusterer = true;
	    double convergenceDelta = 0.001;
	    int numClusters = 100;
	    int maxIterations = 10;
	    if (runClusterer)
	    {
	    	runSequentialKMeansClusterer(conf, output, output1, measure, numClusters, maxIterations, convergenceDelta,fs);
	    }
  }
  public static List<Vector> getPoints(double[][] raw) {
	    List<Vector> points = new ArrayList<Vector>();
	  
	    for (int i = 0; i < raw.length; i++) {
	      double[] fr = raw[i];
	      Vector vec = new RandomAccessSparseVector(fr.length);
	      vec.assign(fr);
	      points.add(vec);
	    }
	    return points;
	  }

  public static void displayCluster(ClusterDumper clusterDumper) throws IOException
  {
      Iterator<Integer> keys = clusterDumper.getClusterIdToPoints().keySet().iterator();
      BufferedWriter bw = new BufferedWriter(new FileWriter("KMeansResult"));
      int num = 0;
      while (keys.hasNext())
      {
          Integer center = keys.next();
          //System.out.println("Center: " + center);
          bw.append(num + ",");
          num ++;
          for (WeightedVectorWritable point : clusterDumper.getClusterIdToPoints().get(center))
          {
        	  Vector v = point.getVector();
              //System.out.println(v.toString().split(":")[0]);
        	  bw.append(v.toString().split(":")[0] + ",");
          }
          bw.append("\n");
      }
      bw.close();
  }
  private static void runSequentialKMeansClusterer(Configuration conf, Path samples, Path output,
    DistanceMeasure measure, int numClusters, int maxIterations, double convergenceDelta,FileSystem fs)
    throws Exception
    {
	    Path clustersIn = new Path(output, "random-seeds");
	    RandomSeedGenerator.buildRandom(conf, samples, clustersIn, numClusters, measure);
	    KMeansDriver.run(samples, clustersIn, output, convergenceDelta, maxIterations, true, 0.0, true);
	    Path outGlobPath = new Path(output, "clusters-*-final");
	    Path clusteredPointsPath = new Path(output,"clusteredPoints");
	    System.out.printf("Dumping out clusters from clusters: %s and clusteredPoints: %s\n", outGlobPath, clusteredPointsPath);
	
	    ClusterDumper clusterDumper = new ClusterDumper(outGlobPath, clusteredPointsPath);
		displayCluster(clusterDumper);
    }
}