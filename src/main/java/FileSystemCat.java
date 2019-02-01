import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;


/**
 * @Author: xu.dm
 * @Date: 2019/1/31 14:39
 * @Description:
 */
public class FileSystemCat {

    private static String HDFSUri = "hdfs://bigdata-senior01.home.com:9000";

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        //文本文件cat
        fileCat(args);

        //文件copy
//        fileCopyWithProgress(args);

        //file status
//        fileStatus(args);

        // file status pattern
//        filePattern(args);

        long endTime = System.currentTimeMillis();
        long timeSpan = endTime - startTime;
        System.out.println("耗费时间：" + timeSpan + "毫秒");
    }

    private static FileSystem getFileSystem() {
        Configuration conf = new Configuration();


        //文件系统
        FileSystem fs = null;
        String hdfsUri = HDFSUri;
        if (StringUtils.isBlank(hdfsUri)) {
            //返回默认文件系统，如果在hadoop集群下运行，使用此种方法可直接获取默认文件系统；
            try {
                fs = FileSystem.get(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            //返回指定的文件系统，如果在本地测试，需要此种方法获取文件系统；
            try {
                URI uri = new URI(hdfsUri.trim());
                fs = FileSystem.get(uri, conf);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return fs;
    }


    ///hadoop输出文本文件内容
    private static void fileCat(String[] args) throws Exception {
        String uri = args[0];
        Configuration conf = new Configuration();
//        conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.defaultFS","hdfs://bigdata-senior01.home.com:9000");
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
//        InputStream in  = null;
//        FSDataInputStream继承 java.io.DataInputStream,支持随机访问
        FSDataInputStream in = null;
        try {
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
            in.seek(0);
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }


    //将本地文件拷贝到hadoop文件系统上
    //只能在hadoop的节点机上运行
    private static void fileCopyWithProgress(String[] args) throws Exception {
        String locaSrc = args[0];
        String dst = args[1];

        Configuration conf = new Configuration();
        conf.set("fs.DefaultFs", "hdfs://bigdata-senior01.home.com:9000");

        FileSystem fs = FileSystem.get(URI.create(dst), conf);


//        fs.copyFromLocalFile(new Path(locaSrc),new Path(dst));
//        fs.close();
//        System.out.println("copyFromLocalFile...done");


        InputStream in = new BufferedInputStream(new FileInputStream(locaSrc));

        FSDataOutputStream out = null;

        out = fs.create(new Path(dst), () -> System.out.print("."));
        IOUtils.copyBytes(in, out, 4096, true);
        if (fs != null)
            fs.close();
    }

    //查找文件，递归列出给定目录或者文件的属性
    private static void fileStatus(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://bigdata-senior01.home.com:9000");
        Path file = new Path(args[0]);
        FileSystem fs = null;
        try {
//            fs = FileSystem.get(URI.create(args[0]),conf);
            fs = FileSystem.get(conf);

//            fs = getFileSystem();

            //对单个文件或目录
//            FileStatus fileStatus = fs.getFileStatus(file);

            //对单个文件或目录下所有文件和目录
            FileStatus[] fileStatuses = fs.listStatus(file);

            //FileUtil封装了很多文件功能
            //FileStatus[] 和 Path[]转换
//            Path[] files = FileUtil.stat2Paths(fileStatuses);

            for (FileStatus fileStatus : fileStatuses) {
                System.out.println("-------------->");
                System.out.println("是否目录：" + fileStatus.isDirectory());
                System.out.println("path:" + fileStatus.getPath().toString());
                System.out.println("length:" + fileStatus.getLen());
                System.out.println("accessTime:" + LocalDateTime.ofInstant(Instant.ofEpochMilli(fileStatus.getAccessTime()), ZoneId.systemDefault()));
                System.out.println("permission:" + fileStatus.getPermission().toString());
                //递归查找子目录
                if (fileStatus.isDirectory()) {
                    FileSystemCat.fileStatus(new String[]{fileStatus.getPath().toString()});
                }
            }
        } finally {
            if (fs != null)
                fs.close();
        }
    }

    //查找文件，通配模式，不能直接用于递归，应该作为递归的最外层，不进入递归
    private static void filePattern(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://bigdata-senior01.home.com:9000");
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            //输入参数大于1，第二个参数作为排除参数
            //例如：hadoop jar FileSystemCat.jar /demo ^.*/demo[1-3]$  排除/demo1,/demo2,/demo3
            //例如：hadoop jar FileSystemCat.jar /demo/wc* ^.*/demo/wc3.*$ 排除/demo下wc3开头所有文件
            FileStatus[] fileStatuses = null;
            if (args.length > 1) {
                System.out.println("过滤路径：" + args[1]);
                fileStatuses = fs.globStatus(new Path(args[0]), new RegexExcludePathFilter(args[1]));
            } else {
                fileStatuses = fs.globStatus(new Path(args[0]));
            }

            for (FileStatus fileStatus : fileStatuses) {
                System.out.println("-------------->");
                System.out.println("是否目录：" + fileStatus.isDirectory());
                System.out.println("path:" + fileStatus.getPath().toString());
                System.out.println("length:" + fileStatus.getLen());
                System.out.println("modificationTime:" + LocalDateTime.ofInstant(Instant.ofEpochMilli(fileStatus.getModificationTime()), ZoneId.systemDefault()));
                System.out.println("permission:" + fileStatus.getPermission().toString());
                if (fileStatus.isDirectory()) {
                    FileSystemCat.fileStatus(new String[]{fileStatus.getPath().toString()});
                }
            }
        } finally {
            if (fs != null)
                fs.close();
        }
    }


}
