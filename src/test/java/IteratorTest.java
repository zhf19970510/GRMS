import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class IteratorTest {
    public static void main(String[] args) {
        List<String> list = new ArrayList<>();
        list.add("abc");
        list.add("def");
        list.add("fgh");
        Iterator ite =list.iterator();
        String a = (String)ite.next();
        String b = (String)ite.next();
        String c = (String)ite.next();
        System.out.println(a+" "+b+" "+c);


    }
}
