import org.junit.Test;

public class DemoTest {
    @Test
    public void regexTest(){
        String s = "cat dog,desk push last, this is what. must be";
        String[] ss = s.split("\\W+");
        for(String k: ss) System.out.println(k);
    }
}
