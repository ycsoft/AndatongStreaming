package com.unidt.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by admin on 2018/2/2.
 */

public class HdfsUtils {

    /**
     * 列出某一HDFS目录下的所有文件，只包含文件，不包含目录
     *
     * 该函数主要是为了读取某一目录下的所有可操作的文件块，如果想递归遍历目录，该函数并不合适
     * @param dir
     * @return
     */
    public List<String> listDir(String dir){
        List<String> ret = new ArrayList<String>();
        Configuration conf = new Configuration();

        try {
            FileSystem fs = FileSystem.get(URI.create(dir), conf);
            if(!fs.exists(new Path(dir))) {
                return null;
            }

            FileStatus[] fileStatuses = fs.listStatus( new Path(dir));
            for (int i = 0 ; i < fileStatuses.length; i++) {
                FileStatus status = fileStatuses[i];
                /**
                 * 如果想递归遍历目录下的所有文件及子目录下的文件
                 * 可在此处添加 子目录 的递归调用方式
                 * 安全起见，本项目不做此处理
                 */
                if (!status.isDirectory()) {
                    Path oneFilepath = status.getPath();
                    ret.add(oneFilepath.toUri().getPath());
                    System.out.println(oneFilepath.toUri().getPath());
                }
            }


        } catch (IOException e) {
            e.printStackTrace();
        }

        return ret;
    }

    /**
     * 读取某一HDFS文件块的内容，返回List形式，每个元素为一行的文件内容
     * 注意： 如果文件过大(超过2G)，不建议使用该函数，容易导致JVM堆内存错误(HeapSize not enough)
     * @param dir
     * @return
     */
    public List<String> readFile(String dir){

        Configuration conf = new Configuration();

        try {
            FileSystem fs = FileSystem.get(URI.create(dir), conf);

            Path path = new Path(dir);
            if (!fs.exists(path)){
                System.out.println("Directory not found: " + dir);
                return null;
            }

            FSDataInputStream inputStream = fs.open(path);

            byte[] ioBuffer = new byte[1024];
            int readLen = inputStream.read(ioBuffer);

            StringBuffer sbuf = new StringBuffer();
            while( readLen != -1){
                sbuf.append(new java.lang.String(ioBuffer,0, readLen));
                readLen = inputStream.read(ioBuffer);
            }

            inputStream.close();
            fs.close();
            String sret = sbuf.toString();
            return Arrays.asList(sret.split("\n"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return  null;
    }

    /**
     * 读取某一目录下的文件内容，返回值与readFile相同
     * 实际上该函数循环调用readFile函数完成多个文件内容的读取
     * @param dir
     * @return
     */
    public List<String> readDir(String dir){
        List<String> ret = new ArrayList<String>();
        List<String> paths = listDir(dir);
        if(paths != null && paths.size() > 0) {
            for( String  path:paths){
                System.out.println("Read File:" + path);
                ret.addAll(readFile(path));
            }
        }
        return ret;
    }

    /**
     * 删除HDFS上的某一文件或目录
     * @param toremove
     */
    public void removeDir(String toremove) {
        Configuration conf = new Configuration();
        try {
            FileSystem  fileSystem = FileSystem.get(URI.create(toremove), conf);
            Path rmpath = new Path(toremove);
            if ( fileSystem.exists(rmpath)) {
                fileSystem.deleteOnExit(rmpath);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 上传本地文件只HDFS的指定目录
     * @param src
     * @param dst
     * @throws IOException
     */
    public void uploadLocalFile(String src, String dst) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path srcpath = new Path(src);
        Path dstPath = new Path(dst);

        fs.copyFromLocalFile(srcpath, dstPath);
    }

    public void writeBuff(String path, String content) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path destpath = new Path(path);

        FSDataOutputStream outputStream = fs.append(destpath);
        outputStream.writeBytes(content);
    }
    /**
     * 重命名文件
     * @param oldname
     * @param newName
     */
    public void rename(String oldname, String newName) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path oldpath = new Path(oldname);
        Path newpath = new Path(newName);

        fs.rename(oldpath, newpath);
    }

    /**
     * 创建HDFS目录
     * @param dir
     * @throws IOException
     */
    public void mkdir(String dir) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        fs.mkdirs(new Path(dir));

    }

    public static void main(String[] args){
        HdfsUtils utils = new HdfsUtils();
        List<String> ret =  utils.readDir("/user/xiaodong.yang/log/survey");
    }
}
