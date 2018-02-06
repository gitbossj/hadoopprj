import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import java.io.InputStream;
import java.net.URI;
//test

public class HadoopTest {
    public static void main(String[] args) throws Exception {
        /**指定本地HADOOP程序目录*/
        String LocalHadoopDir = "E:\\hadoop-2.7.5";
        System.setProperty("hadoop.home.dir", LocalHadoopDir);
        /*指定HADOOP服务器执行用户，通常为hdfs*/
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        /*指定hdfs文件系统，CDH端口为8020，其它系统通常为9000*/
        String uri = "hdfs://10.9.175.11:8020/";

        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), config);

        // 列出hdfs上/input目录下的所有文件和目录
        FileStatus[] statuses = fs.listStatus(new Path("/input"));
        for (FileStatus status : statuses) {
            System.out.println(status);
        }

        // 在hdfs的/input目录下创建一个文件，并写入一行文本
        FSDataOutputStream os = fs.create(new Path("/input/test.log"));
        os.write("Hello World!".getBytes());
        os.flush();
        os.close();

        // 显示在hdfs的/input下指定文件的内容
        InputStream is = fs.open(new Path("/input/test.log"));
        IOUtils.copyBytes(is, System.out, 1024, true);
    }

}

