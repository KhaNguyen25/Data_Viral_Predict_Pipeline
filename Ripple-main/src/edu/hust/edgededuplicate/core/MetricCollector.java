package edu.hust.edgededuplicate.core;



import edu.hust.edgededuplicate.core.EdgeServer;
import edu.hust.edgededuplicate.core.NeighborServerInfo;
import edu.hust.edgededuplicate.redis.RedisUtility;
import edu.hust.edgededuplicate.utill.ConfigurationManager;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class MetricCollector {
    // CLOUD_HOP: khoảng cách giả lập tới Cloud.
    // Nếu request không tìm thấy object ở local node hoặc neighbor node,
    // hệ thống gán distance = CLOUD_HOP và xem đó là Cloud Backhaul Request.
    private static final int CLOUD_HOP = ConfigurationManager.getIntProperty("CloudHop");
    // EDGE_HIT_MAX_HOP: số hop tối đa vẫn được xem là phục vụ tại tầng Edge.
    // Ví dụ edgeHitMaxHop=1 nghĩa là local node hoặc neighbor cách 1 hop đều tính là Edge hit.
    private static final int EDGE_HIT_MAX_HOP = ConfigurationManager.getIntProperty("edgeHitMaxHop");
    // THRASH_WINDOW_LINES: cửa sổ quan sát cache thrashing.
    // Nếu object bị evict ở dòng X và được request lại trong vòng THRASH_WINDOW_LINES dòng tiếp theo,
    // đồng thời request đó phải về Cloud, thì tính là 1 lần Cache Thrashing.
    private static final int THRASH_WINDOW_LINES = ConfigurationManager.getIntProperty("thrashWindowLines");
    private static final String OUTPUT = ConfigurationManager.getProperty("metrics.output");

    // totalRequests: tổng số request GET hợp lệ được dùng để đánh giá hệ thống.
    // Chỉ tăng khi RealtimeSendController xử lý một dòng có operation = "get".
    public static long totalRequests = 0;
    // edgeHitRequests: số request được phục vụ tại tầng Edge.
    // Bao gồm Local Hit và Neighbor Hit trong phạm vi EDGE_HIT_MAX_HOP.
    public static long edgeHitRequests = 0;
    // localHitRequests: số request có object ngay tại node nhận request.
    // Điều kiện tính: distance == 0.
    public static long localHitRequests = 0;
    // neighborHitRequests: số request không hit local nhưng tìm được object ở node lân cận.
    // Điều kiện tính: 0 < distance <= EDGE_HIT_MAX_HOP.
    public static long neighborHitRequests = 0;
    // cloudRequests: số request phải truy cập Cloud vì Edge không có object.
    // Điều kiện tính: distance >= CLOUD_HOP.
    public static long cloudRequests = 0;

    // totalLatency: tổng distance của toàn bộ request GET.
    // Dùng để tính Average Latency = totalLatency / totalRequests.
    public static long totalLatency = 0;

    // viralRequests: số request GET truy cập object được Redis đánh dấu viral.
    // Điều kiện tính: RedisUtility.isVideoViral(dataHash) == true.
    public static long viralRequests = 0;
    // viralLatencySum: tổng distance của các request viral.
    // Dùng để tính Viral Average Latency = viralLatencySum / viralRequests.
    public static long viralLatencySum = 0;
    // viralCloudRequests: số request viral nhưng vẫn phải truy cập Cloud.
    // Điều kiện tính: object viral và distance >= CLOUD_HOP.
    public static long viralCloudRequests = 0;

    // evictedFiles: tổng số object bị xóa thành công khỏi cache trong quá trình deduplicate.
    public static long evictedFiles = 0;
    // evictedViralFiles: số object viral bị xóa khỏi cache.
    // Dùng để đánh giá AI có bảo vệ object viral tốt hay không.
    public static long evictedViralFiles = 0;
    // cacheThrashingCount: số lần object bị evict nhưng sau đó lại được request lại gần thời điểm evict
    // và request đó phải về Cloud. Chỉ số càng thấp thì quyết định eviction càng hợp lý.
    public static long cacheThrashingCount = 0;

    // totalStorageSample: tổng các mẫu storage utilization đã lấy trong quá trình chạy.
    // Được scale lên 1_000_000 để tránh sai số khi lưu bằng long.
    public static long totalStorageSample = 0;
    // storageSampleCount: số lần lấy mẫu mức sử dụng cache.
    public static long storageSampleCount = 0;

    public static int currentLineNumber = 0;

    // lastEvictedAtLine: lưu lại object vừa bị evict ở dòng nào.
    // Map này dùng để kiểm tra Cache Thrashing khi object đó được request lại sau eviction.
    private static final Map<Long, Integer> lastEvictedAtLine = new HashMap<>();

    /**
     * Tính distance của một request tới object.
     * Ý nghĩa distance trong hệ thống:
     * - distance = 0: object có ngay tại node nhận request => Local Hit.
     * - 0 < distance <= EDGE_HIT_MAX_HOP: object có ở neighbor node => Neighbor Hit.
     * - distance = CLOUD_HOP: object không có ở Edge => phải truy cập Cloud.
     */
    public static int measureDistance(
            long dataHash,
            EdgeServer requestServer,
            Map<Integer, EdgeServer> serversMap
    ) {
        // Nếu object tồn tại ngay tại node nhận request thì tính Local Hit.
        if (requestServer.searchLocalData(dataHash)) {
            return 0;
        }

        int bestDistance = Integer.MAX_VALUE;

        for (Map.Entry<Integer, NeighborServerInfo> entry : requestServer.neighborInfoMap.entrySet()) {
            int neighborId = entry.getKey();
            int hop = entry.getValue().hop;

            EdgeServer neighbor = serversMap.get(neighborId);
            if (neighbor == null) {
                continue;
            }

            // Nếu object nằm ở neighbor, lấy hop nhỏ nhất làm distance.
            // Đây là cơ sở để tính Neighbor Hit Requests.
            if (neighbor.searchLocalData(dataHash)) {
                bestDistance = Math.min(bestDistance, hop);
            }
        }

        // Nếu không tìm thấy object ở local hoặc neighbor, request phải đi về Cloud.
        if (bestDistance == Integer.MAX_VALUE) {
            return CLOUD_HOP;
        }

        return bestDistance;
    }

    /**
     * Ghi nhận metric cho mỗi request GET hợp lệ.
     * Hàm này là nơi tính các tham số:
     * - Total Requests
     * - Local Hit Requests
     * - Neighbor Hit Requests
     * - Edge Cache Hit Ratio
     * - Cloud Backhaul Requests / Ratio
     * - Average Latency
     * - Viral Requests / Viral Average Latency / Viral Cloud Requests
     * - Cache Thrashing Count
     */
    public static void recordRequest(
            long dataHash,
            int lineNumber,
            int distance
    ) {
        // Total Requests: tăng 1 cho mỗi request GET hợp lệ.
        totalRequests++;

        // Total Latency: cộng distance của request hiện tại.
        // Sau đó Average Latency = totalLatency / totalRequests.
        totalLatency += distance;

        // Local Hit Requests: request được phục vụ ngay tại node nhận request.
        if (distance == 0) {
            localHitRequests++;
        }

        // Neighbor Hit Requests: request được phục vụ bởi node lân cận trong phạm vi Edge.
        if (distance > 0 && distance <= EDGE_HIT_MAX_HOP) {
            neighborHitRequests++;
        }

        // Edge Hit Requests: gồm Local Hit và Neighbor Hit trong phạm vi EDGE_HIT_MAX_HOP.
        // Edge Cache Hit Ratio = edgeHitRequests / totalRequests.
        if (distance <= EDGE_HIT_MAX_HOP) {
            edgeHitRequests++;
        }

        // Cloud Backhaul Requests: request phải về Cloud.
        // Cloud Backhaul Ratio = cloudRequests / totalRequests.
        if (distance >= CLOUD_HOP) {
            cloudRequests++;
        }

        // Kiểm tra object hiện tại có được Redis đánh dấu viral hay không.
        // Redis key thường có dạng: viral:{object_id}=TRUE.
        boolean viral = RedisUtility.isVideoViral(dataHash);

        if (viral) {
            // Viral Requests: số request truy cập object viral.
            viralRequests++;

            // Viral Average Latency = viralLatencySum / viralRequests.
            viralLatencySum += distance;

            // Viral Cloud Requests: request viral nhưng vẫn phải về Cloud.
            if (distance >= CLOUD_HOP) {
                viralCloudRequests++;
            }
        }

        // Cache Thrashing Count:
        // Nếu object từng bị evict tại dòng evictedAt,
        // sau đó được request lại trong vòng THRASH_WINDOW_LINES dòng,
        // và request đó phải về Cloud, thì xem là một lần cache thrashing.
        Integer evictedAt = lastEvictedAtLine.get(dataHash);
        if (evictedAt != null && distance >= CLOUD_HOP) {
            int diff = lineNumber - evictedAt;
            if (diff > 0 && diff <= THRASH_WINDOW_LINES) {
                cacheThrashingCount++;
            }
        }
    }

    /**
     * Ghi nhận metric khi một object bị xóa thành công khỏi cache.
     * Hàm này tính:
     * - Evicted Files
     * - Evicted Viral Files
     * Đồng thời lưu dòng bị xóa để phục vụ tính Cache Thrashing Count sau này.
     */
    public static void recordEviction(long dataHash, int lineNumber) {
        // Evicted Files: tổng số object bị xóa khỏi cache.
        evictedFiles++;

        // Lưu lại object bị xóa tại dòng nào để kiểm tra tái truy cập sau eviction.
        lastEvictedAtLine.put(dataHash, lineNumber);

        // Evicted Viral Files: object bị xóa nhưng đang được Redis đánh dấu viral.
        if (RedisUtility.isVideoViral(dataHash)) {
            evictedViralFiles++;
        }
    }

    /**
     * Lấy mẫu mức sử dụng cache trên toàn bộ Edge Server.
     * Hàm này phục vụ tính Avg Storage Utilization.
     */
    public static void sampleStorage(Map<Integer, EdgeServer> serversMap) {
        long used = 0;
        long capacity = 0;

        for (EdgeServer server : serversMap.values()) {
            // used: tổng số object đang được lưu trong cache của tất cả node.
            used += server.dataTable.size();

            // capacity: tổng dung lượng cache tối đa của tất cả node.
            capacity += server.dataVolume;
        }

        if (capacity > 0) {
            // Storage Utilization tại thời điểm lấy mẫu = used / capacity.
            // Nhân 1_000_000 để lưu bằng long, tránh mất độ chính xác quá sớm.
            totalStorageSample += (used * 1_000_000L / capacity);
            storageSampleCount++;
        }
    }

    // Edge Cache Hit Ratio = edgeHitRequests / totalRequests.
    // Ý nghĩa: tỷ lệ request được xử lý trong phạm vi Edge.
    public static double edgeHitRatio() {
        return totalRequests == 0 ? 0.0 : edgeHitRequests * 1.0 / totalRequests;
    }

    // Cloud Backhaul Ratio = cloudRequests / totalRequests.
    // Ý nghĩa: tỷ lệ request phải truy cập Cloud.
    public static double cloudBackhaulRatio() {
        return totalRequests == 0 ? 0.0 : cloudRequests * 1.0 / totalRequests;
    }

    // Average Latency = totalLatency / totalRequests.
    // Ý nghĩa: độ trễ trung bình của toàn bộ request, tính theo distance/hop.
    public static double avgLatency() {
        return totalRequests == 0 ? 0.0 : totalLatency * 1.0 / totalRequests;
    }

    // Viral Average Latency = viralLatencySum / viralRequests.
    // Ý nghĩa: độ trễ trung bình riêng của nhóm request truy cập object viral.
    public static double avgViralLatency() {
        return viralRequests == 0 ? 0.0 : viralLatencySum * 1.0 / viralRequests;
    }

    // Avg Storage Utilization = trung bình các mẫu (used / capacity).
    // Ý nghĩa: mức sử dụng cache trung bình của toàn bộ hệ thống Edge.
    public static double avgStorageUtilization() {
        return storageSampleCount == 0 ? 0.0 : (totalStorageSample * 1.0 / storageSampleCount) / 1_000_000.0;
    }

    // Dedup Efficiency Score = Edge Cache Hit Ratio / Avg Storage Utilization.
    // Ý nghĩa: mức hiệu quả tạo Edge Hit trên mỗi đơn vị cache đang sử dụng.
    // Chỉ số càng cao nghĩa là hệ thống khai thác dung lượng cache càng hiệu quả.
    public static double dedupEfficiencyScore() {
        double storage = avgStorageUtilization();
        return storage == 0.0 ? 0.0 : edgeHitRatio() / storage;
    }

    public static void printReport() {
        System.out.println("\n" + "=".repeat(20) + " EDGE/AI METRICS " + "=".repeat(20));
        System.out.println("1. Total Requests              : " + totalRequests);
        System.out.println("2. Edge Cache Hit Ratio        : " + String.format("%.6f", edgeHitRatio()));
        System.out.println("   - Local Hit Requests        : " + localHitRequests);
        System.out.println("   - Neighbor Hit Requests     : " + neighborHitRequests);
        System.out.println("3. Cloud Backhaul Requests     : " + cloudRequests);
        System.out.println("4. Cloud Backhaul Ratio        : " + String.format("%.6f", cloudBackhaulRatio()));
        System.out.println("5. Average Latency             : " + String.format("%.6f", avgLatency()));
        System.out.println("6. Viral Requests              : " + viralRequests);
        System.out.println("7. Viral Average Latency       : " + String.format("%.6f", avgViralLatency()));
        System.out.println("8. Viral Cloud Requests        : " + viralCloudRequests);
        System.out.println("9. Evicted Files               : " + evictedFiles);
        System.out.println("10. Evicted Viral Files        : " + evictedViralFiles);
        System.out.println("11. Cache Thrashing Count      : " + cacheThrashingCount);
        System.out.println("12. Avg Storage Utilization    : " + String.format("%.6f", avgStorageUtilization()));
        System.out.println("13. Dedup Efficiency Score     : " + String.format("%.6f", dedupEfficiencyScore()));
    }

    public static void exportCsv() {
        try (PrintWriter pw = new PrintWriter(new FileWriter(OUTPUT))) {
            pw.println("metric,value");
            pw.println("totalRequests," + totalRequests);
            pw.println("edgeHitRatio," + edgeHitRatio());
            pw.println("localHitRequests," + localHitRequests);
            pw.println("neighborHitRequests," + neighborHitRequests);
            pw.println("cloudRequests," + cloudRequests);
            pw.println("cloudBackhaulRatio," + cloudBackhaulRatio());
            pw.println("avgLatency," + avgLatency());
            pw.println("viralRequests," + viralRequests);
            pw.println("avgViralLatency," + avgViralLatency());
            pw.println("viralCloudRequests," + viralCloudRequests);
            pw.println("evictedFiles," + evictedFiles);
            pw.println("evictedViralFiles," + evictedViralFiles);
            pw.println("cacheThrashingCount," + cacheThrashingCount);
            pw.println("avgStorageUtilization," + avgStorageUtilization());
            pw.println("dedupEfficiencyScore," + dedupEfficiencyScore());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void clear() {
        totalRequests = 0;
        edgeHitRequests = 0;
        localHitRequests = 0;
        neighborHitRequests = 0;
        cloudRequests = 0;
        totalLatency = 0;
        viralRequests = 0;
        viralLatencySum = 0;
        viralCloudRequests = 0;
        evictedFiles = 0;
        evictedViralFiles = 0;
        cacheThrashingCount = 0;
        totalStorageSample = 0;
        storageSampleCount = 0;
        lastEvictedAtLine.clear();
    }
}
