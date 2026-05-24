package edu.hust.edgededuplicate.utill;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class ExperimentRecord {

    private final String timestamp;
    private List<Double> fprData;
    private List<Long> memory;
    private double falseDeleteRate;
    private double deduplicateRate;
    private int deletes;
    // private int false_delete;
    private double averageLatency;
    private int root_false;

    private int overhead;


    private int packs;

    public static int Root_False;
    public static int falseDelete;
    public static int allDelete;
    public static int allAdd;
    public static double Latency;

    public static int transHops;

    public static int packSum;

    public static int dupSum;
    public ExperimentRecord(List<Double> fprData, List<Long> memory) {
        // 获取当前时间
        LocalDateTime currentTime = LocalDateTime.now();
        // 定义日期时间格式
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM-dd HH-mm-ss");
        this.timestamp = currentTime.format(formatter) + " " + ConfigurationManager.getProperty("filterClass") + " "
                + ConfigurationManager.getProperty("deduplicateStrategy");
        this.fprData = fprData;
        this.memory = memory;
        this.deletes = allDelete;
        this.averageLatency = Latency;
        //this.false_delete = falseDelete;
        this.root_false = Root_False;
        this.falseDeleteRate = (root_false + 0.0) / allDelete;
        this.deduplicateRate = (allDelete + 0.0) / allAdd;
        this.memory = memory;
        this.overhead = transHops;
        this.packs = packSum;
    }

    public ExperimentRecord() {
        // 获取当前时间
        LocalDateTime currentTime = LocalDateTime.now();
        // 定义日期时间格式
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM-dd HH-mm-ss");
        this.timestamp = currentTime.format(formatter) + " " + ConfigurationManager.getProperty("filterClass") + " "
                + ConfigurationManager.getProperty("deduplicateStrategy");
    }




    public List<Double> getFprData() {
        return fprData;
    }

    public List<Long> getMemory() {
        return memory;
    }

    public double getFalseDeleteRate() {
        return falseDeleteRate;
    }

    public int getDeletes() {
        return deletes;
    }

    public void setDeletes(int deletes) {
        this.deletes = deletes;
    }

    //public int getFalse_delete() {
    //    return false_delete;
    //}

    //public void setFalse_delete(int false_delete) {
    //    this.false_delete = false_delete;
    //}

    public int getRoot_false() {
        return root_false;
    }

    public void setRoot_false(int root_false) {
        this.root_false = root_false;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public double getDeduplicateRate() {
        return deduplicateRate;
    }


    public double getAverageLatency() {
        return averageLatency;
    }

    public static int getFalseDelete() {
        return falseDelete;
    }

    public int getOverhead() {
        return overhead;
    }

    public void setOverhead(int overhead) {
        this.overhead = overhead;
    }

    public int getPacks() {
        return packs;
    }

    public void moveLog() throws IOException {
        String sourceLogPathString = "global.log";
        String sourceConfigPathString = "config.properties";
        String targetFolderPath = "log\\" + this.timestamp;
        // 创建目标文件夹（如果不存在）
        File targetFolder = new File(targetFolderPath);
        if (!targetFolder.exists()) {
            targetFolder.mkdirs();
        }
        // 构建源文件和目标文件的路径
        Path sourceLogPath = Path.of(sourceLogPathString);
        Path sourceConfigPath = Path.of(sourceConfigPathString);
        Path targetPath = Path.of(targetFolderPath, "TestResult.json");
        Path targetConfigPath = Path.of(targetFolderPath, sourceConfigPath.getFileName().toString());

        // 文件路径
        String filePath = "TestResult.json";
        String jsonString = JSON.toJSONString(this, JSONWriter.Feature.PrettyFormat, JSONWriter.Feature.NullAsDefaultValue);

        try {
            // 创建一个FileWriter对象
            FileWriter fileWriter = new FileWriter(targetPath.toString());

            // 将JSON字符串写入文件
            fileWriter.write(jsonString);

            // 关闭文件写入器
            fileWriter.close();

            System.out.println("JSON string has been written to the file successfully.");
        } catch (IOException e) {
            e.printStackTrace();
        }
        Files.copy(sourceConfigPath, targetConfigPath, StandardCopyOption.REPLACE_EXISTING);

    }


    public static void clear(){
        Root_False = 0;
        falseDelete = 0;
        allDelete = 0;
        allAdd = 0;
        Latency = 0;
        transHops = 0;
        packSum = 0;
        dupSum = 0;
    }
    @Override
    public String toString() {
        return "ExperimentRecord{" +
                "timestamp='" + timestamp + '\'' +
                ", falseDeleteRate=" + falseDeleteRate +
                ", deduplicateRate=" + deduplicateRate +
                ", deletes=" + deletes +
                ", averageLatency=" + averageLatency +
                ", root_false=" + root_false +
                ", overhead=" + overhead +
                ", packs=" + packs +
                ", fprData=" + fprData +
                ", memory=" + memory +
                '}';
    }
}
