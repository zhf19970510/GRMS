package com.briup.grms.step4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class IdTag implements WritableComparable<IdTag>{
    private Text id=new Text();
    private IntWritable tag=new IntWritable();
    public IdTag() {
    }

    public IdTag(Text id, IntWritable tag) {
        this.id = new Text(id.toString());
        this.tag = new IntWritable(tag.get());
    }
    public IdTag(String id, int tag) {
        this.id = new Text(id);
        this.tag = new IntWritable(tag);
    }
    public Text getId() {
        return id;
    }

    public void setId(Text id) {
        this.id = new Text(id.toString());
    }

    public IntWritable getTag() {
        return tag;
    }

    public void setTag(IntWritable tag) {
        this.tag = new IntWritable(tag.get());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id.readFields(in);
        this.tag.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.id.write(out);
        this.tag.write(out);
    }

    @Override
    public int compareTo(IdTag o) {
        return this.id.compareTo(o.id)==0?
                this.tag.compareTo(o.tag):
                this.id.compareTo(o.id);
    }

}
