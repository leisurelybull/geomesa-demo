package org.sisyphus.demo.kafka.todo;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class ReadFile {

    /**
     * 读取本地文件
     *
     * @param path 本地文件路径
     * @return
     * @throws IOException
     */
    public static List<String> readLine(String path) throws IOException {
        List<String> result = new ArrayList<>();
        File file = new File(path);
        if (file.exists() && file.isFile()) {
            InputStream in = new FileInputStream(file);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));

            String line = null;
            while ((line = br.readLine()) != null) {
                result.add(line);
            }

            br.close();
            in.close();
        }

        return result;
    }

    public static void main(String[] args) throws IOException {
        List<String> data = readLine("D:\\data\\chinapoi.txt");
        for (int i = 0; i < data.size(); i++) {
            String line = data.get(i);
            if (line != null) {
                String[] split = line.split(",");
                System.out.println(split[0]);
                System.out.println(line);
            }
        }
    }
}
