package edu.hust.edgededuplicate.utill;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Configuration {

    public int numNodes;
    public int numEdges;
    public int dataNum;
    public int hopNum;
    public String filterClass;
    public int power_of_two;
    public int serverVolume ;
    // 在类加载时就创建单例实例
    private static final Configuration INSTANCE = new Configuration();
    // 私有构造函数，禁止外部实例化
    private Configuration() {
        // 防止通过反射方式实例化
        if (INSTANCE != null) {
            throw new IllegalStateException("Already instantiated");
        }
        loadConfig();
    }

    // 提供获取实例的静态方法
    public static Configuration getInstance() {
        return INSTANCE;
    }
    // 可以添加其他业务方法
    private void loadConfig() {
        Properties properties = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                System.out.println("Sorry, unable to find config.properties");
                return;
            }
            // load a properties file from class path, inside static method
            properties.load(input);
            // initialize config with the value from the properties file
            numNodes = Integer.parseInt(properties.getProperty("numNodes"));
            numEdges = Integer.parseInt(properties.getProperty("numEdges"));
            dataNum = Integer.parseInt(properties.getProperty("dataNum"));
            hopNum = Integer.parseInt(properties.getProperty("hopNum"));
            power_of_two = Integer.parseInt(properties.getProperty("power_of_two"));
            serverVolume = Integer.parseInt(properties.getProperty("serverVolume"));
            filterClass = properties.getProperty("filterClass");

        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
