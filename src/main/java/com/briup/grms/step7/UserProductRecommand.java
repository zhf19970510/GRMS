package com.briup.grms.step7;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class UserProductRecommand implements DBWritable,WritableComparable<UserProductRecommand> {
    private Text uid = new Text();
    private Text gid = new Text();
    private IntWritable exp = new IntWritable();

    public UserProductRecommand(){

    }
    public UserProductRecommand(String uid,String gid,int exp){
        this.uid = new Text(uid);
        this.gid = new Text(gid);
        this.exp = new IntWritable(exp);
    }
    public UserProductRecommand(Text uid,Text gid,IntWritable exp){
        this.uid = new Text(uid.toString());
        this.gid = new Text(gid.getBytes());
        this.exp = new IntWritable(exp.get());
    }

    public Text getUid() {
        return uid;
    }

    public Text getGid() {
        return gid;
    }

    public IntWritable getExp() {
        return exp;
    }

    public void setUid(Text uid) {
        this.uid = new Text(uid.toString());
    }

    public void setGid(Text gid) {
        this.gid = new Text(gid.toString());
    }

    public void setExp(IntWritable exp) {
        this.exp = new IntWritable(exp.get());
    }

    @Override
    public int compareTo(UserProductRecommand o) {
        return (this.uid.compareTo(o.uid))==0?
                this.gid.compareTo(o.gid):
                this.uid.compareTo(o.uid);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.uid.write(out);
        this.gid.write(out);
        this.exp.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.uid.readFields(in);
        this.gid.readFields(in);
        this.exp.readFields(in);
    }

    @Override
    public void write(PreparedStatement prep) throws SQLException {
        prep.setString(1,this.uid.toString());
        prep.setString(2,this.gid.toString());
        prep.setInt(3,this.exp.get());
    }

    @Override
    public void readFields(ResultSet rs) throws SQLException {
        if(rs==null){
            return;
        }
        this.uid = new Text(rs.getString(1));
        this.gid = new Text(rs.getString(2));
        this.exp = new IntWritable(rs.getInt(3));

    }

    @Override
    public String toString() {
        return this.uid.toString()+" "+
                this.gid.toString()+" " +
                this.exp.get();
    }
}
