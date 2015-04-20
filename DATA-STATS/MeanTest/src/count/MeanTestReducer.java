package count;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MeanTestReducer extends
		Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	 private DoubleWritable val = new DoubleWritable();
	 private DoubleWritable stddev = new DoubleWritable();
	private double val1[]=new double[4]; 
	private final static Text DEVIATION = new Text("Standard Deviation :");
    ArrayList<Double> nums = new ArrayList<Double>(); 
    
    
	public void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		




    	   
		   double sum = 0;


	        
	        if(key.toString().equalsIgnoreCase("length"))
	        {
	  	      for (DoubleWritable value : values) {
		          sum += value.get();
		          //System.out.println(sum);
		          nums.add(value.get());
		        }

	          System.out.println(sum);
	          System.out.println(nums);
		      
		        val.set(sum);
		        System.out.println("came");
		        context.write(key, val);
	        }
	        
	        if(key.toString().equalsIgnoreCase("count"))
	        {
	  	      for (DoubleWritable value : values) {
		          sum += value.get();
		          //System.out.println(sum);
		        }

	          System.out.println(sum);
		      
		        val.set(sum);
		        context.write(key, val);
                 val1[0]=sum;
                 System.out.println(" count :"+val1[0]);
	        }
	        if(key.toString().equalsIgnoreCase("square"))
	        {
	  	      for (DoubleWritable value : values) {
		          sum += value.get();
		          //System.out.println(sum);
		        }

	          System.out.println(sum);
		      
		        val.set(sum);
	        	val1[1]=sum;
		        context.write(key, val);
                System.out.println("count :"+val1[0]);
                System.out.println("square: "+val1[1]);
                System.out.println("mean: "+val1[2]);
	        }
	        if(key.toString().equalsIgnoreCase("mean"))
	        {
	  	      for (DoubleWritable value : values) {
		          sum += value.get();
		          //System.out.println(sum);
		        }

	          System.out.println(sum);
		      
		        val.set(sum);
                double v=sum/val1[0];
                val.set(v);
		        context.write(key, val);
	        	val1[2]=sum;
                System.out.println("count: "+val1[0]);
                System.out.println("square :"+val1[1]);
                System.out.println("mean: "+val1[2]);
	        }
		      //System.out.println(sum+" =="+i);
	        
	        //this.val1.set(val[i]);
	        //context.write(key, val1);
       
	      
	
	
if(val1[0]!=0&&val1[1]!=0&&val1[2]!=0)
{
	BigDecimal bd2=new BigDecimal(val1[2]);
	System.out.println(bd2.doubleValue());
	System.out.println(val1[2]);
	
	BigDecimal bd=new BigDecimal(val1[0]);
	BigDecimal bd1=new BigDecimal(val1[1]);
		int n=(int)bd.doubleValue();
	System.out.println(n);
	double mean =(bd2.doubleValue())/(bd.doubleValue()); 
		mean=	Math.pow(mean, 2.0);
	System.out.println("mean before"+mean);
    double term = (bd1.doubleValue())/(bd.doubleValue());;
	System.out.println("term before"+term);    
    
    //double stddevq =Math.sqrt((term - mean));
    
    //System.out.println(Math.sqrt(term-mean));
    
    BigDecimal t=new BigDecimal(term);
	
    BigDecimal m=new BigDecimal(mean);
	
	System.out.println("term: after"+t.doubleValue());
	System.out.println("m after"+m.doubleValue()); 
    double value=(t.doubleValue())-(m.doubleValue());

	System.out.println(value);
	
    this.stddev.set(Math.sqrt(value));
    
    System.out.println(Math.sqrt((t.doubleValue()))-(m.doubleValue()));
    
    Collections.sort(nums);
    
    System.out.println(nums);
	context.write(DEVIATION,stddev);
	
	DoubleWritable db=new DoubleWritable();
	db.set(nums.get(0));
	context.write(new Text("MIN"), db);
	
	
	DoubleWritable db2=new DoubleWritable();
	db2.set(nums.get(n-1));
	context.write(new Text("MAX"),db2);
    
	DoubleWritable perc1=new DoubleWritable();
	perc1.set(calPercentile(25,n));
	context.write(new Text("25th Percentile"),perc1);	
	
	DoubleWritable perc2=new DoubleWritable();
	perc2.set(calPercentile(50,n));
	context.write(new Text("50th Percentile"),perc2);
	
	
	
	DoubleWritable perc3=new DoubleWritable();
	perc3.set(calPercentile(75,n));
	context.write(new Text("75th Percentile"),perc3);
	
}	
System.out.println("1");
	        
}
	public double calPercentile(int R,int count)
	{
	System.out.println(R);
	System.out.println(nums);
	System.out.println(count);
		double d3;
		System.out.println(R/100);
		System.out.println(count+1);
		System.out.println((R/100)*(count+1));
		double x=(double)R/100;
		System.out.println(x);
		double d=x*(count+1);
		int s=(int)d;
		System.out.println(d);
		if(d==(double)s)
		{

			d3=nums.get(s-1);
			System.out.println(d3);
						
		}
		else
		{
			double f=d-s;
			int n=(int)d;
			double df1;
			double df2;
			if(nums.get(n-1)>nums.get(n))
			{
				df1=nums.get(n-1);
				df2=nums.get(n);

			}
			else
			{
				df1=nums.get(n);
				df2=nums.get(n-1);

			}
			d3=(f*(df1-df2))+df2;
			System.out.println(d3);
		}
		return d3;
	}
	
}