package edu.hust.edgededuplicate.utill;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;

public class ConfigurationManager {
    private static final String CONFIG_FILE_PATH = "config.properties";
    private static final Properties properties;


    static {
        properties = new Properties();
        loadConfig();
    }

    // 加载配置文件
    private static void loadConfig() {
        try (FileInputStream inputStream = new FileInputStream(CONFIG_FILE_PATH)) {
            properties.load(inputStream);
        } catch (IOException e) {
            // 处理文件加载失败的异常
            e.printStackTrace();
        }
    }

    // 获取配置项的值
    public static String getProperty(String key) {
        return properties.getProperty(key);
    }

    public static int getIntProperty(String key) {
        return Integer.parseInt(properties.getProperty(key));
    }


    public static double getDoubleProperty(String key) {
        return Double.parseDouble(properties.getProperty(key));
    }

    public static int getRandomIntArrayProperty(String key) {
        String[] dataVolumeStringArray = properties.getProperty(key).split(",");
        int[] dataVolumeOptions = new int[dataVolumeStringArray.length];
        for (int i = 0; i < dataVolumeStringArray.length; i++) {
            dataVolumeOptions[i] = Integer.parseInt(dataVolumeStringArray[i]);
        }
        Random random = new Random();
        return dataVolumeOptions[random.nextInt(dataVolumeOptions.length)];
    }

    // 设置配置项的值
    public static void setProperty(String key, String value) {
        properties.setProperty(key, value);
    }

    // 保存配置文件
    public static void saveConfig() {
        try (FileOutputStream outputStream = new FileOutputStream(CONFIG_FILE_PATH)) {
            properties.store(outputStream, null);
        } catch (IOException e) {
            // 处理文件保存失败的异常
            e.printStackTrace();
        }
    }

}

