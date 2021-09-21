package cs4224.utils;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DataLoader implements Closeable {
    private CqlSession session;
    public DataLoader() {
        System.out.println("Working Directory = " + System.getProperty("user.dir"));
        session = CqlSession.builder()
                .withKeyspace(CqlIdentifier.fromCql("wholesale"))
                .build();


        File file = new File("data/temp");
        if (!file.exists()) {
            file.mkdirs();
        }
    }

    public void loadData() {
    }

    public void loadSchema() throws Exception {
        executeCommand("cqlsh -f main\\queries\\create_table.cql --request-timeout=3600");
    }
    private void executeCommand(String command) throws Exception {
        Runtime rt = Runtime.getRuntime();
        Process proc = rt.exec(command);

        BufferedReader inReader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        BufferedReader errReader = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

        String line;
        while ((line = inReader.readLine()) != null) {
            System.out.println(line);
        }

        while ((line = errReader.readLine()) != null) {
            System.out.println(line);
        }
        proc.waitFor();

        inReader.close();
        errReader.close();
    }
    @Override
    public void close() throws IOException {

    }
}
