package edu.hust.edgededuplicate.redis;

import edu.hust.edgededuplicate.utill.ConfigurationManager;
import edu.hust.edgededuplicate.utill.GlobalLogger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import java.util.logging.Logger;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtility {

    private static JedisPool jedisPool;
    protected static final String REDIS_HOST = ConfigurationManager.getProperty("redis.host");
    protected static final int REDIS_PORT = ConfigurationManager.getIntProperty("redis.port");
    static {
        // Cấu hình Pool chịu tải cho môi trường nhiều Edge Server gọi cùng lúc
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(128);
        poolConfig.setMaxIdle(128);
        poolConfig.setMinIdle(16);
        poolConfig.setTestOnBorrow(true);

        jedisPool = new JedisPool(poolConfig, REDIS_HOST, REDIS_PORT);
    }

    /**
     * Hàm này được EdgeServer.java gọi TRƯỚC KHI quyết định xóa file.
     * @param videoId ID của video cần kiểm tra
     * @return true nếu AI của Thành viên 2 đã đánh nhãn là VIRAL
     */
    public static boolean isVideoViral(int videoId) {
        // Cú pháp Key phải THỐNG NHẤT với Thành viên 2 (ví dụ: "viral:123")
//        String key = "viral:" + videoId;
//
//        try (Jedis jedis = jedisPool.getResource()) {
//            // Cách 1: Kiểm tra xem Key có tồn tại không (Nếu T.V 2 chỉ lưu video Viral)
//            // return jedis.exists(key);
//
//            // Cách 2: Kiểm tra theo Value "TRUE" / "FALSE"
//            String status = jedis.get(key);
//            return "TRUE".equals(status);
//
//        } catch (Exception e) {
//            System.err.println("Lỗi kết nối Redis: " + e.getMessage());
//            // Nếu đứt cáp/lỗi Redis, mặc định trả về false để hệ thống Ripple chạy bình thường
//            return false;
//        }
        return false;
    }

}