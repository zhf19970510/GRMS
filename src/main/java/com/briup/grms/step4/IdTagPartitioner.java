package com.briup.grms.step4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class IdTagPartitioner extends Partitioner<IdTag, Text>{

	@Override
	public int getPartition(IdTag key, Text value, int num) {
		String str = key.getId().toString();
		return Math.abs(str.hashCode()%num);
	}

}
