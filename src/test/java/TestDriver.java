import org.junit.internal.TextListener;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

import java.io.File;
import java.util.regex.Pattern;

/**
 * Created by zhihuan1 on 9/5/2016.
 */
public class TestDriver {
    private Pattern testFilterRe = Pattern.compile(".\\.Test.*");
    public static void main(String[] args) {
       /* JUnitCore junit = new JUnitCore();
        junit.addListener(new TextListener(System.out));
        Result result = junit.run();*/
        File dir = new File(".");
        for(File file : dir.listFiles())
        System.out.println(file.getAbsolutePath());
    }

    private boolean isCandidateClass(Class<?> c) {
        return testFilterRe.matcher(c.getName()).find();
    }

/*    private Class<?> getClasses() {

    } */
}
