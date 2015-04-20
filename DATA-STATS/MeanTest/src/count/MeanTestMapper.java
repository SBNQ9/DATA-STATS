package count;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MeanTestMapper extends
		Mapper<Object, Text, Text, DoubleWritable> {


	  private final static Text LENGTH = new Text("length");
	  private final static Text SQUARE = new Text("square");
	  private final static Text COUNT = new Text("count");
	  private final static DoubleWritable ONE = new DoubleWritable(1);


	  private final static Text MEAN = new Text("mean");

	  
	  private DoubleWritable wordLen = new DoubleWritable();
	    private DoubleWritable numSq = new DoubleWritable();
	    private DoubleWritable numMean = new DoubleWritable();
	    //private DoubleWritable sum = new DoubleWritable();
		private int i=0;
		private double sum=0;
	    
	
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		System.out.println("MAP");
		//StringTokenizer itr = new StringTokenizer(value.toString());
		StringTokenizer itr = new StringTokenizer(value.toString());

		//this.sum.set(0);
	      while (itr.hasMoreTokens()) {
	        String string = itr.nextToken();
	        //this.wordLen.set(string.length());

	    	  System.out.println(string);
	    	  
	    	  double b=Double.parseDouble(string);
	    	  this.wordLen.set(b);
	    	  
	    	  
	    	  System.out.println(wordLen);
	    	System.out.println(sum);
	        // the square of an integer is an integer...
	        //this.numSq.set((double) Math.pow(string.length(), 2.0));
	    	
	    	this.numSq.set((double) Math.pow(b, 2.0));
	        this.sum=sum+this.wordLen.get();
	        
	    	System.out.println(sum);
	  
	    	//Sending map values to the Reducer 
            context.write(LENGTH, this.wordLen);
	        context.write(SQUARE, this.numSq);
	        context.write(COUNT, ONE);
	        i++;
	      } 
	      
	      //System.out.println(i);
	      //System.out.println(sum);
	      numMean.set((double)(sum/i));
           context.write(MEAN, this.numMean);
	}

}
