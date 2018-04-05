package Job;



import java.io.IOException;

import javax.naming.Context;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SeqMap {
	static  int profit[]=new int[100];
	   static int job[]=new int[100];
	   static  int dead[]=new int[100];
	   static int temp;
	   static int n;
	   
	   public static abstract class JobsMapper implements Mapper<IntWritable,IntWritable,IntWritable,IntWritable>{
		   public void map(LongWritable key,IntWritable value,Context context)throws IOException{
			   for(int i=0; i<n-1; i++){
			        for(int j=0;j<n-1;j++){
			            if(profit[j+1]>profit[j]){     
			                temp=profit[j+1];
			                profit[j+1]=profit[j];
			                profit[j]=temp;
			     
			                temp=dead[j+1];
			                dead[j+1]=dead[j];
			                dead[j]=temp;
			                
			                temp=job[j+1];
			                job[j+1]=job[j];
			                job[j]=temp;
			            }
			        }
			    }
			    for(int i=0;i<n-1;i++){
			        for(int j=0;j<n-1;j++){
			            if(profit[j]==profit[j+1]){
			                if(dead[j]<dead[j+1]){
			                    temp=dead[j+1];
			                    dead[j+1]=dead[j];
			                    dead[j]=temp;
			                
			                    temp=job[j+1];
			                    job[j+1]=job[j];
			                    job[j]=temp;
			                }
			            }
			        }       
			}
		   }
		   }
	   public void JobsReduce(IntWritable job,Iterable<IntWritable> profit,Context context)throws IOException{
		   SeqMap jb[]=new SeqMap[100];

		   int dmax=0;         //finding the max of deadline
		   int timeslot[]=new int[100];
		   for(int i=1;i<=dmax;i++){
			   	timeslot[i]=-1;
			   }
			   int filledtimeslot=0;

			   //selecting the time slot

			   for(int i=0;i<=n;i++){
			   	int k=minvalue(dmax,dead[i-1]);
			   while(k>=1){
				   if(timeslot[k]==-1){
					   timeslot[k]=i-1;
				
				filledtimeslot  ++;
				break;
			   }
			   k--;
			   }
			   }
			   //to check time slot are filled and then stop it
			   if(filledtimeslot==dmax){
			   break;
			   }

			   //required max profit
			   int maxprofit=0;
			   for(int i=1;i<=dmax;i++){
			   maxprofit=maxprofit+profit[i];
			   context.write(job,new IntWritable(maxprofit));
			   }
			   }
	   private int minvalue(int dmax, int i) {
		if(dmax<i)
			return dmax;
		else
			return i;
	}
	
			   

			   public static void main (String args[])throws Exception {
				   int maxprofit;
			   SeqMap jb[]=new SeqMap[100];
			   Job job=new Job();
			   job.setJarByClass(SeqMap.class);
			   	
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

