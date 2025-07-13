package com.everflowx.esmigration.task;

import com.everflowx.esmigration.websocket.MonitorWebSocketHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 监控信息推送定时任务
 * 
 * @author everflowx
 */
@Slf4j
@Component
public class MonitorPushTask {
    
    @Autowired
    private MonitorWebSocketHandler webSocketHandler;
    
    /**
     * 定时推送系统统计信息 - 每30秒执行一次
     */
    @Scheduled(fixedRate = 30000)
    public void pushSystemStats() {
        try {
            webSocketHandler.pushSystemStats();
        } catch (Exception e) {
            log.debug("推送系统统计信息失败", e);
        }
    }
    
    /**
     * 定时推送任务列表 - 每分钟执行一次
     */
    @Scheduled(fixedRate = 60000)
    public void pushTaskList() {
        try {
            webSocketHandler.pushTaskList();
        } catch (Exception e) {
            log.debug("推送任务列表失败", e);
        }
    }
}