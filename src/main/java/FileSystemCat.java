import org.apache.commons.lang.StringUtils;
//import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;


/**
 * @Author: xu.dm
 * @Date: 2019/1/31 14:39
 * @Description:
 */
public class FileSystemCat {

    private static String HDFSUri = "hdfs://vmhome10.com:9000";

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        //文本文件cat
//        fileCat(args);

        //文件copy，本地到远程
//        fileCopyWithProgress(args);

        //文件copy，远程到本地
//        copyFileToLocal(args);

        //文件写入
//        writeFile(args);

        //读取远程地址，写入本地
//        readFile(args);

        //file status
//        fileStatus(args);

        //查看文件名称、权限、长度、块信息
        fileStatus2(args);

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
        conf.set("fs.defaultFS",HDFSUri);
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
//        InputStream in  = null;
//        FSDataInputStream继承 java.io.DataInputStream,支持随机访问
        FSDataInputStream in = null;
        try {
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);

            //重新定位到文件头，在cat一次
            in.seek(0);
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
            fs.close();
        }
    }


    //将本地文件拷贝到hadoop文件系统上
    //需要开通datanode用于数据传输端口：9866
    private static void fileCopyWithProgress(String[] args) throws Exception {
        String locaSrc = args[0];
        String dst = args[1];

        InputStream in = new BufferedInputStream(new FileInputStream(locaSrc));
        //从windows环境提交的时候需要设置hadoop用户名
        //在linux的其他用户环境下估计也需要
//        System.setProperty("HADOOP_USER_NAME","hadoop");

        Configuration conf = new Configuration();
//        conf.set("fs.DefaultFs", "hdfs://bigdata-senior01.home.com:9000");

          //因为涉及到两个文件系统的数据传输，如果dst不是全路径的话（带不带hdfs的头）用这种方式取到的还是本地文件系统
          //如果不涉及两个文件系统，dst写短路径是没问题的
//        FileSystem fs = FileSystem.get(URI.create(dst), conf);

        //这里如果没有指定用户，就需要使用  System.setProperty("HADOOP_USER_NAME","hadoop");
        FileSystem fs = FileSystem.get(URI.create(HDFSUri),conf,"hadoop");

//        fs.copyFromLocalFile(new Path(locaSrc),new Path(dst));
//        fs.close();
//        System.out.println("copyFromLocalFile...done");




        FSDataOutputStream out = null;

        out = fs.create(new Path(dst), () -> System.out.print("."));

        IOUtils.copyBytes(in, out, 4096, true);
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
        fs.close();
    }

    //hadoop拷贝文件到本地
    private static void copyFileToLocal(String[] args) throws Exception{
        String remoteSrc = args[0];
        String localDest = args[1];

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(HDFSUri),conf);

        fs.copyToLocalFile(new Path(remoteSrc),new Path(localDest));

        FSDataInputStream in = fs.open(new Path(remoteSrc));
        OutputStream out = new BufferedOutputStream(new FileOutputStream(localDest));
        IOUtils.copyBytes(in,out,4096,true);
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
        fs.close();
    }

    //查看文件名称、权限、长度、块信息
    private static void fileStatus2(String[] args) throws Exception{
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS",HDFSUri);

        Path file = new Path(args[0]);

        try (FileSystem fs = FileSystem.get(new URI(HDFSUri),conf,"hadoop")) {
            RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(file, true);
            while (listFiles.hasNext()){
                LocatedFileStatus status = listFiles.next();
                System.out.println("name :"+status.getPath().getName());
                System.out.println("path: "+status.getPath());
                System.out.println("length: "+status.getLen());
                System.out.println("group: "+ status.getGroup());
                System.out.println("permission: "+status.getPermission());
                BlockLocation[] blockLocations = status.getBlockLocations();
                for(BlockLocation location : blockLocations){
                    long length = location.getLength();
                    long offset = location.getOffset();
                    StorageType[] storageTypes = location.getStorageTypes();
                    String[] storageIds = location.getStorageIds();
                    String[] topologyPaths = location.getTopologyPaths();
                    String[] names = location.getNames();
                    String[] hosts = location.getHosts();
                    for(int i=0;i<hosts.length;i++){
                        System.out.println("length: "+length+"  offset: "+offset+"  host: "+hosts[i]);
                    }
                }
                System.out.println("---------------------------------");
            }
        }
    }

    //查找文件，递归列出给定目录或者文件的属性
    private static void fileStatus(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", HDFSUri);
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
                System.out.println("path: " + fileStatus.getPath().toString());
                System.out.println("name: " + fileStatus.getPath().getName());
                System.out.println("length: " + fileStatus.getLen());
                System.out.println("accessTime: " + LocalDateTime.ofInstant(Instant.ofEpochMilli(fileStatus.getAccessTime()), ZoneId.systemDefault()));
                System.out.println("permission: " + fileStatus.getPermission().toString());
                System.out.println("replication: "+fileStatus.getReplication());
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
        conf.set("fs.defaultFS", HDFSUri);
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

    //往远程服务器上写一个文件
    private static void writeFile(String[] args) throws Exception{
        Path f = new Path(args[0]);
        Text inputString = new Text(args[1]);
        Configuration conf = new Configuration();
        //只保存两个副本
        conf.set("dfs.replication","2");
//        conf.set("fs.defaultFS",HDFSUri);
        FileSystem fs = FileSystem.get(new URI(HDFSUri), conf,"hadoop");
        FSDataOutputStream out =  fs.create(f,true);
        byte[] bytes = serialize(inputString);
        out.write(bytes);
        out.close();
        fs.close();
    }

    //远程文件读取，写入本地
    private static void readFile(String[] args) throws IOException{
        Path remoteSource = new Path(args[0]);
        String localDest = args[1];

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS",HDFSUri);
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream in = fs.open(remoteSource);
        OutputStream out = new BufferedOutputStream(new FileOutputStream(localDest));
        Writable writable = new Text();
        byte[] bytes = new byte[1024];
        for(int bytesRead=in.read(bytes);bytesRead>0;bytesRead=in.read(bytes)){
            System.out.println(new String(bytes));
            out.write(bytes,0,bytesRead);

        }

//        int length = in.read(bytes,0,4096);
//        System.out.println("length: "+length+",byte[]: \n"+new String(bytes));
//        bytes = derialize(writable,bytes);
//        System.out.println("writable:\n"+writable.toString()+"\nbyte[]: \n"+new String(bytes));
        in.close();
        fs.close();
        out.close();

    }

    private static byte[] serialize(Writable writable) throws IOException{
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(out);
        writable.write(dataOutputStream);
        dataOutputStream.close();
        return out.toByteArray();
    }

    private static byte[] derialize(Writable writable,byte[] bytes) throws IOException{
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);

        DataInputStream dataIn = new DataInputStream(in);
        writable.readFields(dataIn);
        dataIn.close();
        return  bytes;
    }


}
