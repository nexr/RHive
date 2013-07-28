package com.nexr.rhive.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;

public class JobManager {
	private String user;
	
	public JobManager() {
		user = System.getProperty("user.name");
	}
	 
	public List<JobStatus> listJobs(boolean all) throws IOException {
		JobClient jobClient = new JobClient(new JobConf());
		List<JobStatus> list = new ArrayList<JobStatus>();
		
		JobStatus[] jobs = null;
		if (all) {
			jobs = jobClient.getAllJobs();
		} else {
			jobs = jobClient.jobsToComplete();
		}
		
		ClusterStatus clusterStatus = jobClient.getClusterStatus();
		clusterStatus.getUsedMemory();
		
		if (jobs != null) {
			for (JobStatus jobStatus : jobs) {
				if (user.equals(jobStatus.getUsername())) {
					list.add(jobStatus);
					JobID jobID = jobStatus.getJobID();
					
					RunningJob job = jobClient.getJob(jobID);
					String jobName = job.getJobName();
					
				}
			}
		}
		
		return list;
	}
	
	public boolean kill(String id) throws IOException {
		JobClient jobClient = new JobClient(new JobConf());
		
		JobID jobID = JobID.forName(id);
		RunningJob job = jobClient.getJob(jobID);
		if (job == null) {
			return false;
		}
		
		job.killJob();
		
		return true;
	}
}