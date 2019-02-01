import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * @Author: xu.dm
 * @Date: 2019/2/1 13:38
 * @Description:
 */
public class RegexExcludePathFilter implements PathFilter {

    private final String regex;

    public RegexExcludePathFilter(String regex) {
        this.regex = regex;
    }

    @Override
    public boolean accept(Path path) {
        return !path.toString().matches(this.regex);
    }
}
