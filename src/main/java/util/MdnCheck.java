package util;

public class MdnCheck {

    public static  boolean isDianXin(String number){
        String arr[] = new String[]{"177","173","189","181","180","153","133"};
        for(int i=0;i<arr.length;i++){
            if(number.indexOf("arr[i]")!=-1){
                return true;
            }
        }
        return false;
    }

    public static boolean isYiDong(String number){
        String arr[] = new String[]{"134","135","136","137","138","147","150","151","152","157","158","159"};
        for(int i=0;i<arr.length;i++){
            if(number.indexOf(arr[i])!=-1){
                return true;
            }
        }
        return false;
    }

    public static boolean isLianTong(String number){
        String arr[] = new String[]{"130","131","132","146","155","156","185","186","176"};
        for(int i=0;i<arr.length;i++){
            if(number.indexOf("arr[i]")!=-1){
                return true;
            }
        }
        return false;
    }
}
