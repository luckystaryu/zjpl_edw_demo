package utils;

/***
 *
 */
public class getSplitKeysWithReginNum {
    public static byte[][] getSplitKeysWithReginNum(int amount,int regionNum) {
        int total = amount;//总数据量
        int num = regionNum;//分区数
        //步长
        int step = total / num;
        //多余的步长
        int tail = total % num;
        //标准步长和非标准步长分界点
        //为了减少最后一个分区与其他分区的区间差别
        int split_step = num - tail - 1;
        //总位数
        int length = Integer.toString(total-step).length();
        //标准位数
        String format ="%0"+length+"d";
        //第一个分界点
        int limit =step;
        //准备预分区数组
        byte[][] splitKeys = new byte[num-1][];
        //遍历
        for(int i=0;i<num-1;i++){
            if(i <split_step){
                if(Integer.toString(limit).length()<length){
                    splitKeys[i] =String.format(format,limit).getBytes();
                }else{
                    splitKeys[i] =Integer.toString(limit).getBytes();
                }
                limit +=step;
            }else{
                if(Integer.toString(limit).length()<length){
                    splitKeys[i] = String.format(format,limit).getBytes();
                }else{
                    splitKeys[i] = Integer.toString(limit).getBytes();
                }
                limit +=(step+1);
            }
        }
        return splitKeys;
    }
}
