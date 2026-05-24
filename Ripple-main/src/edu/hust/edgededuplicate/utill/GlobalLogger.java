package edu.hust.edgededuplicate.utill;


import java.util.logging.*;

public class GlobalLogger {
    private static Logger logger = null;



    public static Logger getLogger() {
        if (logger == null) {
            logger = Logger.getLogger(GlobalLogger.class.getName());

            // 禁用父记录器的处理程序传递
            logger.setUseParentHandlers(false);

            // 配置处理程序和级别
            try {


                // 创建自定义的控制台处理程序
                Handler consoleHandler = new ConsoleHandler() {
                    @Override
                    public void publish(LogRecord record) {
                        Level level = record.getLevel();
                        String colorCode = "";

                        if (level == Level.SEVERE) {
                            colorCode = "\u001B[31m"; // Red color for SEVERE messages
                        } else if (level == Level.WARNING) {
                            colorCode = "\u001B[33m"; // Yellow color for WARNING messages
                        } else if (level == Level.INFO) {
                            colorCode = "\u001B[37m"; // White color for INFO messages
                        }
                        // 带信息头
                        //String formattedMessage = colorCode + super.getFormatter().format(record) + "\u001B[0m"; // Reset color
                        // 不带信息头
                        String formattedMessage = colorCode + record.getMessage() + "\u001B[0m\n";
                        System.out.print(formattedMessage);
                    }
                };

                // 设置自定义处理程序的格式
                consoleHandler.setFormatter(new SimpleFormatter());

                // 添加处理程序到 Logger
                logger.addHandler(consoleHandler);
                logger.setLevel(Level.INFO);
            } catch (Exception e) {
                logger.log(Level.SEVERE, "An error occurred in getLogger()", e);
            }
        }
        return logger;
    }

    public static void main(String[] args) {
        Logger logger = GlobalLogger.getLogger();

        logger.severe("This is a severe error message.");
        logger.warning("This is a warning message.");
        logger.info("This is an informational message.");
    }
}