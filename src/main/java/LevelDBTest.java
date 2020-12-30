import org.iq80.leveldb.*;
import static org.fusesource.leveldbjni.JniDBFactory.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @Author: ssingualrity
 * @Date: 2020/12/29 20:33
 */
public class LevelDBTest {
    public static void main(String[] args) throws IOException {
        Options options = new Options();
        options.createIfMissing(true);
        DB db = factory.open(new File("C:\\Users\\thinkpad\\Desktop\\db"), options);
        try {
            // Use the db in here....
            System.out.println(new String(db.get("test".getBytes(StandardCharsets.UTF_8))));
        } finally {
            // Make sure you close the db to shutdown the
            // database and avoid resource leaks.
            db.close();
        }
    }
}
