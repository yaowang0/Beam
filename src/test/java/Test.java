import java.io.UnsupportedEncodingException;

/**
 * Created by YaoWang on 2018/1/2.
 *
 */
public class Test {

    public static void main(String[] args) throws UnsupportedEncodingException {

        char c = 100;
        String string = "1000000000";
        System.out.println(string.getBytes().length);
        System.out.println(string.length());
        System.out.println("测试".getBytes("GBK").length);
    }
}
