package sqoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.Sqoop;
import org.apache.sqoop.tool.SqoopTool;
import org.apache.sqoop.util.OptionsFileUtil;

public class SqoopTest {
    private static int importDataFromMysql() throws Exception{
        String[] args = new String[]{
                "--connect", "jdbc:mysql://172.16.0.55:3306/materialfac",
                "--driver","com.mysql.jdbc.Driver",
                "-username","root",
                "-password","concom603",
                "--table","tFacMaterialBase",
                "-m","10",
                "--target-dir","/user/yuxuqi/tFacMaterialBase"
        };
        String[] expandArguments = OptionsFileUtil.expandArguments(args);
        SqoopTool tool = SqoopTool.getTool("import");
        Configuration conf = new Configuration();
        conf.set("fs.default.name","hdfs://192.168.1.61:8020");//设置HDFS服务器地址
        Configuration loadPlugins = SqoopTool.loadPlugins(conf);
        Sqoop sqoop = new Sqoop((com.cloudera.sqoop.tool.SqoopTool) tool,loadPlugins);
        return Sqoop.runSqoop(sqoop,expandArguments);
    }

    public static void main(String[] args) throws Exception {
        importDataFromMysql();
    }
}
