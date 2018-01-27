 /* 50% is done coding on process uploading ASAP*/import java.io.IOException;
import java.util.*;

import javax.naming.Context;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Jobs {
	static  int profit[]=new int[100];
	   static int task[]=new int[100];
	   static  int d[]=new int[100];
	   
	   public static abstract class JobsMapper implements Mapper<IntWritable,IntWritable,IntWritable,IntWritable>{
		   public void map(LongWritable key,IntWritable value,Context context)throws IOException{
		   }
		   }


		   public static abstract class JobsReduce implements Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
		    
		   private int[] jb;

		public void reduce(IntWritable task,Iterable<IntWritable> profit,Context context)throws IOException{
			   int n;
		   for(int i=1;i<n;i++) 	//sort the profit
		   {
			   for(int j=0;j<n-i;j++)
			   {
			   if(profit[j+1]>profit[j])
			   {
			   int temp=jb[j+1];
			   jb[j+1]=jb[j];
			   jb[j]=temp;
			   }
			   }
			   }

			   int dmax=0;         //finding the max of deadline
			   for(int i=0;i<n;i++){
			   if(jb[i].d >dmax)
			   dmax=jb[i].d;
			   }

			   //initialing the time slot = -1
			   for(int i=1;i<=dmax;i++){
			   int timeslot[i]=-1;
			   }
			   int filledtimeslot=0;

			   //selecting the timeslot

			   for(int i=0;i<=n;i++){
			   	int k=minvalue(dmax,jb[i-1].d);
			   while(k>=1){
			   if(timeslot[k]==-1){
			   timeslot[k]=i-1;
			   int fillestimeslot  =0;
			fillestimeslot  ++;
			   break;
			   }
			   k--;
			   }
			   }
			   //to ckeck timeslot are filled and then stop it
			   if(filledtimeslot==dmax){
			   break;
			   }

			   //required max profit
			   int maxprofit=0;
			   for(int i=1;i<=dmax;i++){
			   maxprofit +=jb[timeslot[i].profit;
			   context.write(task,new IntWritable(maxprofit));
			   }
			   }
			   }

			   public static void main (String args[])throws Exception {
			   Jobs jb[]=new Jobs[100];
			   Job job=new Job();
			   job.setJarByClass(Jobs.class);
			   	
			   job.setMapperClass(JobsMapper.class);
			   	
			   job.setReducerClass(JobsReduce.class);


			   	
			   job.setMapOutputKeyClass(IntWritable.class);
			   	
			   job.setMapOutputValueClass(IntWritable.class);

			   	
			   job.setOutputKeyClass(IntWritable.class);
			   	
			   job.setOutputValueClass(IntWritable.class);



			   System.out.println("maximum profit is"+maxprofit);   //display max profit	
			   FileInputFormat.addInputPath(job, new Path(args[0]));
			          
			    FileOutputFormat.setOutputPath(job, new Path(args[1]));

			   	
			   job.waitForCompletion(true);
			       
			   }

			   }
