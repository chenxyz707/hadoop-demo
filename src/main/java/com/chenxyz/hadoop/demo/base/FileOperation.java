package com.chenxyz.hadoop.demo.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

/**
 * 使用HadoopAPI实现文件操作
 */
public class FileOperation {

    public static final String ROOT_PATH = "H://hadoop/";

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop");
//        listDir();
        uploadFileToHdfs();
    }


    //2：上传文件
    public static void uploadFileToHdfs() throws Exception{
        //针对这种权限问题，有集中解决方案，这是一种，还可以配置hdfs的xml文件来解决
        //System.setProperty("HADOOP_USER_NAME","hadoop") ;

        //FileSystem是一个抽象类，我们可以通过查看源码来了解
        String path = "hdfs://192.168.174.128:9000" ;
        URI uri = new URI(path) ;//创建URI对象
        FileSystem fs = FileSystem.get(uri, new Configuration()) ;//获取文件系统
        //创建源地址
        Path src = new Path(ROOT_PATH + "/fileoperation/test.txt") ;
        //创建目标地址
        Path dst = new Path("/") ;
        //调用文件系统的复制函数，前面的参数是指是否删除源文件，true为删除，否则不删除
        fs.copyFromLocalFile(false, src, dst);
        //最后关闭文件系统
        System.out.println("=========文件上传成功==========");
        fs.close();//当然这里我们在正式书写代码的时候需要进行修改，在finally块中关闭
    }

    //下载文件
    public static void downFromHdfs() throws Exception{
        String path = "hdfs://192.168.174.128:9000";
        URI uri = new URI(path) ;
        FileSystem fs = FileSystem.get(uri, new Configuration()) ;

        //Hadoop文件系统中通过Hadoop Path对象来代表一个文件
        Path src = new Path("/tfiles/a.txt") ;
        FSDataInputStream in = fs.open(src);

        File targetFile = new File("d://aa.txt") ;
        FileOutputStream out = new FileOutputStream(targetFile) ;
        //IOUtils是Hadoop自己提供的工具类，在编程的过程中用的非常方便
        //最后那个参数就是是否使用完关闭的意思
        IOUtils.copyBytes(in, out, 4096, true);
        System.out.println("=========文件下载成功=========");
    }


    //遍历文件系统的某个目录
    public static void listDir(){

        String path = "hdfs://192.168.174.128:9000" ;
        //创建URI对象
        URI uri = null ;
        FileSystem fs = null ;
        try {
            uri = new URI(path) ;
            fs = FileSystem.get(uri, new Configuration()) ;
            //输入要遍历的目录路径
            Path dst = new Path("/") ;
            //调用listStatus()方法获取一个文件数组
            //FileStatus对象封装了文件的和目录的元数据，包括文件长度、块大小、权限等信息
            FileStatus[] liststatus = fs.listStatus(dst) ;
            for (FileStatus ft : liststatus) {
                //判断是否是目录
                String isDir = ft.isDirectory()?"文件夹":"文件" ;
                //获取文件的权限
                String permission = ft.getPermission().toString() ;
                //获取备份块
                short replication = ft.getReplication() ;
                //获取数组的长度
                long len = ft.getLen() ;
                //获取文件的路径
                String filePath = ft.getPath().toString() ;
                System.out.println("文件信息：");
                System.out.println("是否是目录？ "+isDir);
                System.out.println("文件权限 "+permission);
                System.out.println("备份块 "+replication);
                System.out.println("文件长度  "+len);
                System.out.println("文件路劲  "+filePath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
