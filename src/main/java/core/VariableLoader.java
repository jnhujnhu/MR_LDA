package core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Scanner;

import static utils.Settings.*;

/**
 * Created by Kevin on 12/12/2016.
 */
public class VariableLoader {

    public static int[][] Load_C(String C_Path) throws IOException {
        int[][] C = new int[Vocabulary_No + 1][K + 1];
        FileSystem fileSystem = FileSystem.get(new Configuration());
        int K_pos = 0, V_pos = 0;
        if(fileSystem.exists(new Path(C_Path))) {
            FileStatus[] status_list = fileSystem.listStatus(new Path(C_Path));
            if (status_list != null) {
                for (FileStatus status : status_list) {
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem
                            .open(status.getPath())));
                    Scanner scanner = new Scanner(bufferedReader);
                    while (scanner.hasNext()) {
                        int num = scanner.nextInt();
                        C[V_pos + 1][K_pos + 1] = num;
                        K_pos++;
                        V_pos += K_pos / K;
                        K_pos %= K;
                    }
                }
            }
        }
        else {
            fileSystem.mkdirs(new Path(C_Path));
        }
        return C;
    }

    public static HashMap<String, Integer> Load_WordMap(String WM_Path) throws IOException {
        HashMap<String, Integer> VID = new HashMap<String, Integer>();
        FileSystem fileSystem = FileSystem.get(new Configuration());
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem
                .open(new Path(WM_Path))));
        Scanner scanner = new Scanner(bufferedReader);
        while (scanner.hasNext()) {
            String word = scanner.next();
            int id = scanner.nextInt();
            VID.put(word, id);
        }
        return VID;
    }

}
