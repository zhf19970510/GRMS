import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ListComparatorTest {
    public static void main(String[] args) {
        List<String>list = Arrays.asList("abc","gkc","fdji","ifo");
        Collections.sort(list,(o1,o2)->(o1.compareTo(o2)));
        System.out.println(list);
    }
}
