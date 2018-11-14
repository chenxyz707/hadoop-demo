package com.chenxyz.hadoop.demo.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 将log文件按照日期合并后上传至HDFS
 */
public class LogUpload {

    public static final String ROOT_PATH = "H://hadoop/";

    public static final String COMBINE_DIR = ROOT_PATH + "fileoperation/logcombine/";

    public static void main(String[] args) throws IOException, URISyntaxException {

        // 创建合并后文件的存放目录
        File combineDir = new File(COMBINE_DIR);
        if (combineDir.exists()) {
            combineDir.delete();
        }
        combineDir.mkdir();


        String path = "hdfs://192.168.174.128:9000";
        URI uri = new URI(path);
        FileSystem fs = FileSystem.get(uri, new Configuration());

        Path src = new Path(ROOT_PATH + "fileoperation/logdatas/");
        final FileSystem fileSystem = src.getFileSystem(new Configuration());
        final FileStatus[] fileStatuses = fileSystem.listStatus(src);
        for (FileStatus status : fileStatuses) {
            if (status.isDirectory()) {
                String cfn = status.getPath().getName().replace("-", "");
                File combineFile = new File(ROOT_PATH + "fileoperation/logcombine/" + cfn + ".txt");
                combineFile.createNewFile();
                FileOutputStream out = new FileOutputStream(combineFile);
                Path dir = status.getPath();
                fileSystem.globStatus(new Path("*.txt"));
                final FileStatus[] logStatuses = fileSystem.listStatus(dir);
                for (FileStatus logStatus : logStatuses) {
                    System.out.println(logStatus.getPath().toString());
                    final Path logPath = logStatus.getPath();
                    final FSDataInputStream in = fileSystem.open(logPath);
//                    BufferedReader br = new BufferedReader(new InputStreamReader(in));

                    IOUtils.copyBytes(in, out, 4096);
                    try {
                        in.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                try {
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


    }
}
