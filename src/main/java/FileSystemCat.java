import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;


/**
 * @Author: xu.dm
 * @Date: 2019/1/31 14:39
 * @Description:
 */
public class FileSystemCat {
    public static void main(String[] args) throws Exception{
        //文本文件cat
        //fileCat(args);


        fileCopyWithProgress(args);
    }

    ///hadoop输出文本文件内容
    private static void fileCat(String[] args) throws Exception{
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri),conf);
//        InputStream in  = null;
//        FSDataInputStream继承 java.io.DataInputStream,支持随机访问
        FSDataInputStream in = null;
        try {
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in,System.out,4096,false);
            in.seek(0);
            IOUtils.copyBytes(in,System.out,4096,false);
        }finally {
            IOUtils.closeStream(in);
        }
    }


    //将本地文件拷贝到hadoop文件系统上
    private static void fileCopyWithProgress(String[] args) throws Exception {
        String locaSrc = args[0];
        String dst = args[1];

        InputStream in = new BufferedInputStream(new FileInputStream(locaSrc));

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst),conf);
//        OutputStream out = null;
        FSDataOutputStream out = null;
//        out = fs.create(new Path(dst), new Progressable() {
//            @Override
//            public void progress()  {
//                System.out.print(".");
//            }
//        });
        out = fs.create(new Path(dst), () -> System.out.print("."));
        IOUtils.copyBytes(in,out,4096,true);
    }
}
