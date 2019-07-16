package utils;

import java.security.MessageDigest;

public class MD5Utils {
    public static String String2DM5(String inputStr){
        MessageDigest md5 =null;
        try{
            md5=MessageDigest.getInstance("MD5");
        }catch (Exception e){
            System.out.println(e.toString());
            return null;
        }
        char[] charArray = inputStr.toCharArray();//将字符串转为字符数组
        byte[] byteArray = new byte[charArray.length];//创建字节数组

        for(int i=0;i<charArray.length;i++){
            byteArray[i] =(byte) charArray[i];
        }
        //将得到的字节数组进行MD5运算
        byte[] md5Bytes = md5.digest(byteArray);
        StringBuffer md5Str = new StringBuffer();
        for(int i=0;i<md5Bytes.length;i++){
            if(Integer.toHexString(0xFF & md5Bytes[i]).length() ==1)
                md5Str.append("0").append(Integer.toHexString(0xFF &md5Bytes[i]));
            else
                md5Str.append(Integer.toHexString(0xFF & md5Bytes[i]));
        }
        return md5Str.toString();
    }
}
