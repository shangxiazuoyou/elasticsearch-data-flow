// ES数据迁移监控面板JavaScript
// 增强版本：包含更详细的错误处理和用户友好的提示

// 错误处理工具类
class ErrorHandler {
    static showError(error, context = '') {
        console.error('Error occurred:', error);
        
        let message = '发生未知错误';
        let suggestion = '请稍后重试或联系管理员';
        let errorCode = 'UNKNOWN_ERROR';
        
        if (error.response) {
            // HTTP响应错误
            const data = error.response.data || {};
            message = data.message || `HTTP ${error.response.status} 错误`;
            suggestion = data.suggestion || suggestion;
            errorCode = data.errorCode || `HTTP_${error.response.status}`;
        } else if (error.errorCode) {
            // 自定义业务异常
            message = error.message;
            suggestion = error.suggestion || suggestion;
            errorCode = error.errorCode;
        } else if (error.message) {
            // JavaScript错误
            message = error.message;
        }
        
        this.showErrorModal({
            errorCode: errorCode,
            message: message,
            suggestion: suggestion,
            context: context,
            traceId: error.traceId || 'N/A',
            timestamp: error.timestamp || Date.now()
        });
    }
    
    static showErrorModal(errorInfo) {
        const modalHtml = `
            <div class="modal fade" id="errorModal" tabindex="-1">
                <div class="modal-dialog">
                    <div class="modal-content">
                        <div class="modal-header bg-danger text-white">
                            <h5 class="modal-title">
                                <i class="fas fa-exclamation-triangle"></i> 操作失败
                            </h5>
                            <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal"></button>
                        </div>
                        <div class="modal-body">
                            <div class="alert alert-danger" role="alert">
                                <h6><i class="fas fa-bug"></i> 错误代码: ${errorInfo.errorCode}</h6>
                                <p class="mb-2">${errorInfo.message}</p>
                                <small class="text-muted">追踪ID: ${errorInfo.traceId}</small>
                            </div>
                            
                            <div class="alert alert-info" role="alert">
                                <h6><i class="fas fa-lightbulb"></i> 解决建议</h6>
                                <p class="mb-0">${errorInfo.suggestion}</p>
                            </div>
                            
                            ${errorInfo.context ? `
                                <div class="alert alert-secondary" role="alert">
                                    <h6><i class="fas fa-info-circle"></i> 操作上下文</h6>
                                    <p class="mb-0">${errorInfo.context}</p>
                                </div>
                            ` : ''}
                            
                            <div class="accordion" id="errorDetails">
                                <div class="accordion-item">
                                    <h2 class="accordion-header">
                                        <button class="accordion-button collapsed" type="button" 
                                                data-bs-toggle="collapse" data-bs-target="#errorTechnical">
                                            技术详情
                                        </button>
                                    </h2>
                                    <div id="errorTechnical" class="accordion-collapse collapse">
                                        <div class="accordion-body">
                                            <small>
                                                <strong>时间:</strong> ${new Date(errorInfo.timestamp).toLocaleString()}<br>
                                                <strong>错误码:</strong> ${errorInfo.errorCode}<br>
                                                <strong>追踪ID:</strong> ${errorInfo.traceId}
                                            </small>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div class="modal-footer">
                            <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">关闭</button>
                            <button type="button" class="btn btn-primary" onclick="ErrorHandler.copyErrorInfo('${errorInfo.traceId}')">
                                <i class="fas fa-copy"></i> 复制错误信息
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        `;
        
        // 移除旧的错误模态框
        const existingModal = document.getElementById('errorModal');
        if (existingModal) {
            existingModal.remove();
        }
        
        // 添加新的错误模态框
        document.body.insertAdjacentHTML('beforeend', modalHtml);
        const modal = new bootstrap.Modal(document.getElementById('errorModal'));
        modal.show();
    }
    
    static copyErrorInfo(traceId) {
        const errorText = `错误追踪ID: ${traceId}\n时间: ${new Date().toLocaleString()}`;
        
        if (navigator.clipboard) {
            navigator.clipboard.writeText(errorText).then(() => {
                this.showToast('错误信息已复制到剪贴板', 'success');
            });
        } else {
            // 降级处理
            const textArea = document.createElement('textarea');
            textArea.value = errorText;
            document.body.appendChild(textArea);
            textArea.select();
            document.execCommand('copy');
            document.body.removeChild(textArea);
            this.showToast('错误信息已复制到剪贴板', 'success');
        }
    }
    
    static showToast(message, type = 'info', duration = 3000) {
        const toastHtml = `
            <div class="toast" role="alert" style="position: fixed; top: 20px; right: 20px; z-index: 9999;">
                <div class="toast-header">
                    <i class="fas fa-${type === 'success' ? 'check-circle text-success' : 
                                     type === 'warning' ? 'exclamation-triangle text-warning' : 
                                     type === 'error' ? 'times-circle text-danger' : 
                                     'info-circle text-info'}"></i>
                    <strong class="me-auto ms-2">系统提示</strong>
                    <button type="button" class="btn-close" data-bs-dismiss="toast"></button>
                </div>
                <div class="toast-body">
                    ${message}
                </div>
            </div>
        `;
        
        document.body.insertAdjacentHTML('beforeend', toastHtml);
        const toastElement = document.body.lastElementChild;
        const toast = new bootstrap.Toast(toastElement, { delay: duration });
        toast.show();
        
        // 自动清理
        setTimeout(() => {
            if (toastElement.parentNode) {
                toastElement.remove();
            }
        }, duration + 500);
    }
    
    static handleApiResponse(response, context = '') {
        if (!response.ok) {
            throw {
                response: {
                    status: response.status,
                    data: null
                },
                message: `HTTP ${response.status} 错误`,
                context: context
            };
        }
        return response.json().then(data => {
            if (!data.success) {
                throw {
                    errorCode: data.errorCode || 'API_ERROR',
                    message: data.message || '接口调用失败',
                    suggestion: data.suggestion || '请检查请求参数或稍后重试',
                    traceId: data.traceId,
                    timestamp: data.timestamp,
                    context: context
                };
            }
            return data;
        });
    }
}

// API调用封装
class ApiClient {
    static async get(url, context = '', retries = 3) {
        for (let attempt = 1; attempt <= retries; attempt++) {
            try {
                const controller = new AbortController();
                const timeoutId = setTimeout(() => controller.abort(), 10000); // 10秒超时
                
                const response = await fetch(url, {
                    signal: controller.signal,
                    headers: {
                        'X-Request-ID': `${Date.now()}-${Math.random()}`
                    }
                });
                
                clearTimeout(timeoutId);
                return await ErrorHandler.handleApiResponse(response, context);
                
            } catch (error) {
                console.error(`API请求失败 (尝试 ${attempt}/${retries}):`, error);
                
                if (attempt === retries) {
                    ErrorHandler.showError(error, context);
                    throw error;
                }
                
                // 指数退避重试
                await new Promise(resolve => setTimeout(resolve, Math.pow(2, attempt) * 1000));
            }
        }
    }
    
    static async post(url, data = {}, context = '') {
        try {
            const response = await fetch(url, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(data)
            });
            return await ErrorHandler.handleApiResponse(response, context);
        } catch (error) {
            ErrorHandler.showError(error, context);
            throw error;
        }
    }
    
    static async delete(url, context = '') {
        try {
            const response = await fetch(url, { method: 'DELETE' });
            return await ErrorHandler.handleApiResponse(response, context);
        } catch (error) {
            ErrorHandler.showError(error, context);
            throw error;
        }
    }
}

class MigrationMonitor {
    constructor() {
        this.stompClient = null;
        this.isConnected = false;
        this.tasks = new Map();
        this.autoRefreshEnabled = true;
        this.refreshInterval = null;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 5;
        this.reconnectDelay = 5000;
        
        this.init();
        this.bindPageUnloadEvents();
    }
    
    init() {
        // 确保DOM完全加载后再初始化图表
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', () => {
                this.initializeAfterDOM();
            });
        } else {
            this.initializeAfterDOM();
        }
    }
    
    initializeAfterDOM() {
        this.connectWebSocket();
        this.loadInitialData();
        this.startAutoRefresh();
        this.bindEvents();
    }
    
    // WebSocket连接
    connectWebSocket() {
        const socket = new SockJS('/ws/monitor');
        this.stompClient = Stomp.over(socket);
        
        this.stompClient.connect({}, (frame) => {
            console.log('WebSocket连接成功: ' + frame);
            this.isConnected = true;
            this.addLog('WebSocket连接成功', 'success');
            
            // 订阅任务相关消息
            this.stompClient.subscribe('/topic/tasks', (message) => {
                this.handleTaskMessage(JSON.parse(message.body));
            });
            
            // 订阅系统统计消息
            this.stompClient.subscribe('/topic/system', (message) => {
                this.handleSystemMessage(JSON.parse(message.body));
            });
            
            // 订阅系统资源消息
            this.stompClient.subscribe('/topic/system-resources', (message) => {
                this.handleSystemResourceMessage(JSON.parse(message.body));
            });
            
            // 订阅完整统计报告
            this.stompClient.subscribe('/topic/comprehensive-stats', (message) => {
                this.handleComprehensiveStatsMessage(JSON.parse(message.body));
            });
            
            // 订阅性能告警
            this.stompClient.subscribe('/topic/alerts', (message) => {
                this.handlePerformanceAlert(JSON.parse(message.body));
            });
            
            // 订阅健康状态
            this.stompClient.subscribe('/topic/health', (message) => {
                this.handleHealthStatusMessage(JSON.parse(message.body));
            });
            
            // 订阅批量更新
            this.stompClient.subscribe('/topic/batch', (message) => {
                this.handleBatchUpdateMessage(JSON.parse(message.body));
            });
            
            // 订阅错误消息
            this.stompClient.subscribe('/topic/errors', (message) => {
                this.handleErrorMessage(JSON.parse(message.body));
            });
            
            // 订阅警告消息
            this.stompClient.subscribe('/topic/warnings', (message) => {
                this.handleWarningMessage(JSON.parse(message.body));
            });
            
        }, (error) => {
            console.error('WebSocket连接失败: ' + error);
            this.isConnected = false;
            this.addLog('WebSocket连接失败: ' + error, 'error');
            
            // 智能重连机制
            this.handleReconnect();
        });
    }
    
    // 处理任务消息
    handleTaskMessage(message) {
        console.log('收到任务消息:', message);
        
        switch (message.eventType) {
            case 'TASK_LIST':
                this.updateTaskList(message.tasks);
                break;
            case 'TASK_CREATED':
            case 'TASK_STARTED':
            case 'TASK_PROGRESS':
            case 'TASK_COMPLETED':
            case 'TASK_CANCELLED':
            case 'TASK_PAUSED':
            case 'TASK_RESUMED':
                this.updateSingleTask(message.task);
                this.addLog(`任务 ${message.task.taskId}: ${message.eventType}`, 'info');
                break;
            case 'TASK_DELETED':
                this.removeTask(message.task.taskId);
                this.addLog(`任务 ${message.task.taskId} 已删除`, 'warning');
                break;
        }
    }
    
    // 处理系统消息
    handleSystemMessage(message) {
        console.log('收到系统消息:', message);
        if (message.eventType === 'SYSTEM_STATS') {
            this.updateSystemStats(message.stats);
        } else if (message.eventType === 'ENHANCED_SYSTEM_STATS') {
            this.updateEnhancedSystemStats(message);
        } else if (message.eventType === 'PERFORMANCE_ALERT') {
            this.handlePerformanceAlert(message);
        }
    }
    
    // 处理错误消息
    handleErrorMessage(message) {
        this.addLog(`错误: ${message.errorMessage}`, 'error');
    }
    
    // 处理警告消息
    handleWarningMessage(message) {
        this.addLog(`警告: ${message.warningMessage}`, 'warning');
    }
    
    // 显示/隐藏加载指示器
    showLoading(show = true) {
        const indicator = document.getElementById('loadingIndicator');
        if (indicator) {
            indicator.style.display = show ? 'block' : 'none';
        }
    }
    
    // 加载初始数据
    async loadInitialData() {
        try {
            this.showLoading(true);
            
            // 立即尝试初始化图表
            this.initializeCharts();
            
            // 如果立即初始化失败，等待一秒后再试
            setTimeout(() => {
                console.log('延迟初始化图表...');
                this.initializeCharts();
            }, 1000);
            
            // 加载任务列表
            const tasksResponse = await fetch('/api/monitor/tasks');
            if (tasksResponse.ok) {
                const tasksData = await tasksResponse.json();
                this.updateTaskList(tasksData.tasks);
            }
            
            // 加载系统统计
            const statsResponse = await fetch('/api/monitor/stats/enhanced');
            if (statsResponse.ok) {
                const statsData = await statsResponse.json();
                if (statsData.success && statsData.taskStats) {
                    this.updateTaskStatistics(statsData.taskStats);
                }
                if (statsData.success && statsData.performanceStats) {
                    this.updatePerformanceDisplay(statsData.performanceStats);
                }
                if (statsData.success && statsData.realtimeMetrics) {
                    this.updateRealtimeMetrics(statsData.realtimeMetrics);
                }
                
                // 更新图表（使用获取到的数据）
                if (statsData.success) {
                    this.updateCharts(statsData);
                }
            }
            
        } catch (error) {
            console.error('加载初始数据失败:', error);
            this.addLog('加载初始数据失败: ' + error.message, 'error');
        } finally {
            this.showLoading(false);
        }
    }
    
    // 更新任务列表
    updateTaskList(tasks) {
        const tasksList = document.getElementById('tasksList');
        const noTasks = document.getElementById('noTasks');
        
        if (!tasks || tasks.length === 0) {
            tasksList.innerHTML = '';
            noTasks.style.display = 'block';
            return;
        }
        
        noTasks.style.display = 'none';
        tasksList.innerHTML = '';
        
        tasks.forEach(task => {
            this.tasks.set(task.taskId, task);
            this.renderTaskCard(task);
        });
    }
    
    // 更新单个任务
    updateSingleTask(task) {
        this.tasks.set(task.taskId, task);
        
        const existingCard = document.getElementById(`task-${task.taskId}`);
        if (existingCard) {
            existingCard.remove();
        }
        
        this.renderTaskCard(task);
        
        // 如果任务列表为空，隐藏"暂无任务"提示
        const noTasks = document.getElementById('noTasks');
        if (this.tasks.size > 0) {
            noTasks.style.display = 'none';
        }
    }
    
    // 移除任务
    removeTask(taskId) {
        this.tasks.delete(taskId);
        const taskCard = document.getElementById(`task-${taskId}`);
        if (taskCard) {
            taskCard.remove();
        }
        
        // 如果没有任务了，显示"暂无任务"提示
        if (this.tasks.size === 0) {
            document.getElementById('noTasks').style.display = 'block';
        }
    }
    
    // 渲染任务卡片
    renderTaskCard(task) {
        const tasksList = document.getElementById('tasksList');
        const taskCard = document.createElement('div');
        taskCard.className = 'col-md-6 col-lg-4 mb-3';
        taskCard.id = `task-${task.taskId}`;
        
        const statusClass = task.status.toLowerCase();
        const statusIcon = this.getStatusIcon(task.status);
        const progressPercent = Math.round(task.progressPercentage || 0);
        
        taskCard.innerHTML = `
            <div class="card task-card ${statusClass}">
                <div class="card-header d-flex justify-content-between align-items-center">
                    <h6 class="mb-0">${task.taskName || task.taskId}</h6>
                    <span class="badge bg-${this.getStatusColor(task.status)}">
                        ${statusIcon} ${task.status}
                    </span>
                </div>
                <div class="card-body">
                    <div class="mb-2">
                        <small class="text-muted">类型:</small> 
                        <span class="badge bg-secondary">${task.taskType}</span>
                    </div>
                    
                    ${task.currentPhase ? `
                        <div class="mb-2">
                            <small class="text-muted">阶段:</small>
                            <span class="phase-badge badge bg-info">${task.currentPhase}</span>
                        </div>
                    ` : ''}
                    
                    <div class="mb-2">
                        <div class="d-flex justify-content-between">
                            <small>进度</small>
                            <small>${progressPercent}%</small>
                        </div>
                        <div class="progress">
                            <div class="progress-bar ${task.status === 'RUNNING' ? 'progress-bar-striped progress-bar-animated' : ''}" 
                                 style="width: ${progressPercent}%"></div>
                        </div>
                    </div>
                    
                    <div class="row text-center">
                        <div class="col">
                            <small class="text-muted">总数</small><br>
                            <strong>${this.formatNumber(task.totalDocuments || 0)}</strong>
                        </div>
                        <div class="col">
                            <small class="text-muted">已处理</small><br>
                            <strong>${this.formatNumber(task.processedDocuments || 0)}</strong>
                        </div>
                        <div class="col">
                            <small class="text-muted">耗时</small><br>
                            <strong>${this.formatDuration(task.durationMs || 0)}</strong>
                        </div>
                    </div>
                    
                    <div class="mt-3 d-flex justify-content-between">
                        <button class="btn btn-sm btn-outline-primary" onclick="monitor.showTaskDetail('${task.taskId}')">
                            <i class="fas fa-eye"></i> 详情
                        </button>
                        <div>
                            ${this.renderTaskActions(task)}
                        </div>
                    </div>
                </div>
            </div>
        `;
        
        tasksList.appendChild(taskCard);
    }
    
    // 渲染任务操作按钮
    renderTaskActions(task) {
        const actions = [];
        
        if (task.status === 'RUNNING') {
            actions.push(`<button class="btn btn-sm btn-warning me-1" onclick="monitor.pauseTask('${task.taskId}')">
                <i class="fas fa-pause"></i>
            </button>`);
            actions.push(`<button class="btn btn-sm btn-danger" onclick="monitor.cancelTask('${task.taskId}')">
                <i class="fas fa-stop"></i>
            </button>`);
        } else if (task.status === 'PAUSED') {
            actions.push(`<button class="btn btn-sm btn-success me-1" onclick="monitor.resumeTask('${task.taskId}')">
                <i class="fas fa-play"></i>
            </button>`);
            actions.push(`<button class="btn btn-sm btn-danger" onclick="monitor.cancelTask('${task.taskId}')">
                <i class="fas fa-stop"></i>
            </button>`);
        } else if (task.status === 'COMPLETED' || task.status === 'FAILED' || task.status === 'CANCELLED') {
            actions.push(`<button class="btn btn-sm btn-outline-danger" onclick="monitor.deleteTask('${task.taskId}')">
                <i class="fas fa-trash"></i>
            </button>`);
        }
        
        return actions.join('');
    }
    
    // 更新系统统计
    updateSystemStats(stats) {
        document.getElementById('totalTasks').textContent = stats.totalTasks || 0;
        document.getElementById('runningTasks').textContent = stats.runningTasks || 0;
        document.getElementById('completedTasks').textContent = stats.completedTasks || 0;
        document.getElementById('failedTasks').textContent = stats.failedTasks || 0;
        document.getElementById('pendingTasks').textContent = stats.pendingTasks || 0;
        document.getElementById('totalDocs').textContent = this.formatNumber(stats.totalProcessedDocuments || 0);
    }
    
    // 更新增强版系统统计
    updateEnhancedSystemStats(message) {
        console.log('更新增强版系统统计:', message);
        // 更新任务统计
        if (message.taskStats) {
            console.log('更新任务统计:', message.taskStats);
            this.updateTaskStatistics(message.taskStats);
        }
        
        // 更新性能统计
        if (message.performanceStats) {
            console.log('更新性能统计:', message.performanceStats);
            this.updatePerformanceDisplay(message.performanceStats);
        }
        
        // 更新实时指标
        if (message.realtimeMetrics) {
            console.log('更新实时指标:', message.realtimeMetrics);
            this.updateRealtimeMetrics(message.realtimeMetrics);
        }
    }
    
    // 处理系统资源消息
    handleSystemResourceMessage(message) {
        if (message.type === 'SYSTEM_RESOURCES') {
            this.updateSystemResourceDisplay(message.resourceStats);
            this.updateRealtimeMetrics(message.realtimeMetrics);
        }
    }
    
    // 处理完整统计报告
    handleComprehensiveStatsMessage(message) {
        if (message.type === 'COMPREHENSIVE_REPORT') {
            this.updateComprehensiveStatsDisplay(message.stats);
        }
    }
    
    // 处理性能告警
    handlePerformanceAlert(message) {
        if (message.eventType === 'PERFORMANCE_ALERT') {
            this.showPerformanceAlert(message);
        }
    }
    
    // 处理健康状态消息
    handleHealthStatusMessage(message) {
        if (message.eventType === 'HEALTH_STATUS') {
            this.updateHealthDisplay(message);
        }
    }
    
    // 处理批量更新消息
    handleBatchUpdateMessage(message) {
        if (message.eventType === 'BATCH_UPDATE' && message.updates) {
            Object.keys(message.updates).forEach(key => {
                this.handleSpecificUpdate(key, message.updates[key]);
            });
        }
    }
    
    // 更新任务统计显示
    updateTaskStatistics(taskStats) {
        // 更新基础统计
        document.getElementById('totalTasks').textContent = taskStats.totalTasks || 0;
        document.getElementById('runningTasks').textContent = taskStats.runningTasks || 0;
        document.getElementById('completedTasks').textContent = taskStats.completedTasks || 0;
        document.getElementById('failedTasks').textContent = taskStats.failedTasks || 0;
        document.getElementById('pendingTasks').textContent = taskStats.pendingTasks || 0;
        
        // 更新文档统计
        if (document.getElementById('totalDocs')) {
            document.getElementById('totalDocs').textContent = this.formatNumber(taskStats.totalDocuments || 0);
        }
        if (document.getElementById('processedDocs')) {
            document.getElementById('processedDocs').textContent = this.formatNumber(taskStats.processedDocuments || 0);
        }
        if (document.getElementById('successDocs')) {
            document.getElementById('successDocs').textContent = this.formatNumber(taskStats.successDocuments || 0);
        }
        if (document.getElementById('failedDocs')) {
            document.getElementById('failedDocs').textContent = this.formatNumber(taskStats.failedDocuments || 0);
        }
        
        // 更新成功率和完成率
        if (document.getElementById('successRate')) {
            document.getElementById('successRate').textContent = taskStats.successRate || '0.00%';
        }
        if (document.getElementById('completionRate')) {
            document.getElementById('completionRate').textContent = taskStats.completionRate || '0.00%';
        }
        
        // 更新平均任务时长
        if (document.getElementById('avgTaskDuration')) {
            document.getElementById('avgTaskDuration').textContent = taskStats.averageTaskDuration || '暂无数据';
        }
    }
    
    // 更新性能显示
    updatePerformanceDisplay(performanceStats) {
        if (document.getElementById('currentTotalSpeed')) {
            document.getElementById('currentTotalSpeed').textContent = performanceStats.currentTotalSpeed || '0 文档/秒';
        }
        if (document.getElementById('currentAverageSpeed')) {
            document.getElementById('currentAverageSpeed').textContent = performanceStats.currentAverageSpeed || '0 文档/秒';
        }
        if (document.getElementById('estimatedCompletion')) {
            document.getElementById('estimatedCompletion').textContent = performanceStats.estimatedCompletionTime || '计算中...';
        }
        if (document.getElementById('overallThroughput')) {
            document.getElementById('overallThroughput').textContent = performanceStats.overallThroughput || '暂无数据';
        }
        if (document.getElementById('totalProcessingTime')) {
            document.getElementById('totalProcessingTime').textContent = performanceStats.totalProcessingTimeHours || '0.00小时';
        }
    }
    
    // 更新系统资源显示
    updateSystemResourceDisplay(resourceStats) {
        // 内存统计
        if (resourceStats.memory) {
            const memory = resourceStats.memory;
            if (document.getElementById('heapUsage')) {
                document.getElementById('heapUsage').textContent = `${memory.heapUsed}MB / ${memory.heapMax}MB`;
            }
            if (document.getElementById('heapUsagePercent')) {
                document.getElementById('heapUsagePercent').textContent = memory.heapUsagePercent || '0.00%';
                
                // 更新进度条
                const progressBar = document.querySelector('#heapUsageBar .progress-bar');
                if (progressBar) {
                    const percent = parseFloat(memory.heapUsagePercent?.replace('%', '') || '0');
                    progressBar.style.width = percent + '%';
                    progressBar.className = `progress-bar ${percent > 80 ? 'bg-danger' : percent > 60 ? 'bg-warning' : 'bg-success'}`;
                }
            }
            if (document.getElementById('nonHeapUsage')) {
                document.getElementById('nonHeapUsage').textContent = `${memory.nonHeapUsed}MB`;
            }
        }
        
        // GC统计
        if (resourceStats.garbageCollection) {
            const gc = resourceStats.garbageCollection;
            if (document.getElementById('totalGcTime')) {
                document.getElementById('totalGcTime').textContent = gc.totalGcTime || '0ms';
            }
            if (document.getElementById('totalGcCount')) {
                document.getElementById('totalGcCount').textContent = gc.totalGcCount || '0';
            }
        }
        
        // 线程统计
        if (resourceStats.threads) {
            const threads = resourceStats.threads;
            if (document.getElementById('currentThreadCount')) {
                document.getElementById('currentThreadCount').textContent = threads.currentThreadCount || '0';
            }
            if (document.getElementById('peakThreadCount')) {
                document.getElementById('peakThreadCount').textContent = threads.peakThreadCount || '0';
            }
        }
        
        // 运行时统计
        if (resourceStats.runtime) {
            const runtime = resourceStats.runtime;
            if (document.getElementById('availableProcessors')) {
                document.getElementById('availableProcessors').textContent = runtime.availableProcessors || '0';
            }
            if (document.getElementById('totalMemory')) {
                document.getElementById('totalMemory').textContent = `${runtime.totalMemoryMB}MB`;
            }
            if (document.getElementById('freeMemory')) {
                document.getElementById('freeMemory').textContent = `${runtime.freeMemoryMB}MB`;
            }
        }
    }
    
    // 更新实时指标
    updateRealtimeMetrics(realtimeMetrics) {
        if (document.getElementById('systemLoadAverage')) {
            document.getElementById('systemLoadAverage').textContent = realtimeMetrics.systemLoadAverage || '不支持';
        }
        
        if (document.getElementById('serverTime')) {
            document.getElementById('serverTime').textContent = realtimeMetrics.serverTime || '未知';
        }
        
        if (document.getElementById('systemUptime')) {
            const uptimeSeconds = realtimeMetrics.uptime || 0;
            document.getElementById('systemUptime').textContent = this.formatDuration(uptimeSeconds * 1000);
        }
        
        // 更新活跃任务指标
        if (realtimeMetrics.activeTasks) {
            this.updateActiveTasksDisplay(realtimeMetrics.activeTasks);
        }
    }
    
    // 更新活跃任务显示
    updateActiveTasksDisplay(activeTasks) {
        const container = document.getElementById('activeTasksContainer');
        if (!container) return;
        
        if (Object.keys(activeTasks).length === 0) {
            container.innerHTML = '<div class="text-muted">暂无活跃任务</div>';
            return;
        }
        
        let html = '';
        Object.keys(activeTasks).forEach(taskId => {
            const task = activeTasks[taskId];
            html += `
                <div class="mb-2 p-2 border rounded">
                    <div class="d-flex justify-content-between">
                        <small><strong>${taskId.substring(0, 8)}...</strong></small>
                        <small>${task.phase || '处理中'}</small>
                    </div>
                    <div class="progress mb-1" style="height: 4px;">
                        <div class="progress-bar" style="width: ${task.progress || 0}%"></div>
                    </div>
                    <div class="d-flex justify-content-between">
                        <small>速度: ${task.speed || 0} docs/s</small>
                        <small>时长: ${this.formatDuration((task.duration || 0) * 1000)}</small>
                    </div>
                </div>
            `;
        });
        
        container.innerHTML = html;
    }
    
    // 显示性能告警
    showPerformanceAlert(alertMessage) {
        const alertHtml = `
            <div class="alert alert-${alertMessage.severity === 'CRITICAL' ? 'danger' : 
                                      alertMessage.severity === 'WARNING' ? 'warning' : 'info'} alert-dismissible fade show" 
                 role="alert" style="position: fixed; top: 70px; right: 20px; z-index: 9999; min-width: 300px;">
                <h6><i class="fas fa-exclamation-triangle"></i> 性能告警</h6>
                <strong>${alertMessage.alertType}:</strong> ${alertMessage.message}
                ${alertMessage.metrics ? `<hr><small>${JSON.stringify(alertMessage.metrics, null, 2)}</small>` : ''}
                <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
            </div>
        `;
        
        document.body.insertAdjacentHTML('beforeend', alertHtml);
        
        // 自动清理
        setTimeout(() => {
            const alert = document.body.lastElementChild;
            if (alert && alert.classList.contains('alert')) {
                alert.remove();
            }
        }, 10000);
        
        // 记录到日志
        this.addLog(`性能告警: ${alertMessage.alertType} - ${alertMessage.message}`, 'warning');
    }
    
    // 更新健康状态显示
    updateHealthDisplay(healthData) {
        // 健康状态指示器
        const healthIndicator = document.getElementById('healthIndicator');
        if (healthIndicator) {
            const status = healthData.status || 'UNKNOWN';
            healthIndicator.className = `badge bg-${status === 'UP' ? 'success' : status === 'DOWN' ? 'danger' : 'warning'}`;
            healthIndicator.textContent = status;
        }
        
        // 更新健康检查详情
        if (healthData.details) {
            this.updateHealthDetails(healthData.details);
        }
    }
    
    // 处理特定更新
    handleSpecificUpdate(updateType, updateData) {
        switch (updateType) {
            case 'taskStats':
                this.updateTaskStatistics(updateData);
                break;
            case 'performanceStats':
                this.updatePerformanceDisplay(updateData);
                break;
            case 'resourceStats':
                this.updateSystemResourceDisplay(updateData);
                break;
            default:
                console.log('未知更新类型:', updateType);
        }
    }
    
    // 更新完整统计显示
    updateComprehensiveStatsDisplay(stats) {
        // 更新所有统计组件
        if (stats.taskStats) {
            this.updateTaskStatistics(stats.taskStats);
        }
        if (stats.systemResources) {
            this.updateSystemResourceDisplay(stats.systemResources);
        }
        if (stats.performanceStats) {
            this.updatePerformanceDisplay(stats.performanceStats);
        }
        if (stats.realtimeMetrics) {
            this.updateRealtimeMetrics(stats.realtimeMetrics);
        }
        
        // 更新异常统计
        if (stats.exceptionStats) {
            this.updateExceptionStats(stats.exceptionStats);
        }
        
        // 更新趋势数据
        if (stats.trendData) {
            this.updateTrendDisplay(stats.trendData);
        }
        
        this.addLog('完整统计报告已更新', 'info');
    }
    
    // 更新异常统计
    updateExceptionStats(exceptionStats) {
        if (document.getElementById('totalExceptions')) {
            document.getElementById('totalExceptions').textContent = exceptionStats.totalExceptions || 0;
        }
        if (document.getElementById('recentExceptions')) {
            document.getElementById('recentExceptions').textContent = exceptionStats.recentExceptions || 0;
        }
    }
    
    // 更新趋势显示
    updateTrendDisplay(trendData) {
        if (document.getElementById('trendDataPoints')) {
            document.getElementById('trendDataPoints').textContent = trendData.dataPoints || 0;
        }
        
        // 这里可以集成图表库来显示趋势数据
        console.log('趋势数据已更新:', trendData);
    }
    
    // 显示任务详情
    async showTaskDetail(taskId) {
        try {
            const response = await fetch(`/api/monitor/tasks/${taskId}`);
            if (response.ok) {
                const data = await response.json();
                if (data.success) {
                    this.renderTaskDetailModal(data.task);
                    const modal = new bootstrap.Modal(document.getElementById('taskDetailModal'));
                    modal.show();
                } else {
                    this.addLog(`获取任务详情失败: ${data.message}`, 'error');
                }
            }
        } catch (error) {
            console.error('获取任务详情失败:', error);
            this.addLog('获取任务详情失败: ' + error.message, 'error');
        }
    }
    
    // 渲染任务详情模态框
    renderTaskDetailModal(task) {
        const content = document.getElementById('taskDetailContent');
        content.innerHTML = `
            <div class="row">
                <div class="col-md-6">
                    <h6>基本信息</h6>
                    <table class="table table-sm">
                        <tr><td>任务ID:</td><td>${task.taskId}</td></tr>
                        <tr><td>任务名称:</td><td>${task.taskName || '未设置'}</td></tr>
                        <tr><td>任务类型:</td><td>${task.taskType}</td></tr>
                        <tr><td>状态:</td><td><span class="badge bg-${this.getStatusColor(task.status)}">${task.status}</span></td></tr>
                        <tr><td>创建时间:</td><td>${new Date(task.createTime).toLocaleString()}</td></tr>
                        <tr><td>开始时间:</td><td>${task.startTime ? new Date(task.startTime).toLocaleString() : '未开始'}</td></tr>
                        <tr><td>结束时间:</td><td>${task.endTime ? new Date(task.endTime).toLocaleString() : '未结束'}</td></tr>
                        <tr><td>运行时长:</td><td>${this.formatDuration(task.durationMs || 0)}</td></tr>
                    </table>
                </div>
                <div class="col-md-6">
                    <h6>进度信息</h6>
                    <table class="table table-sm">
                        <tr><td>进度:</td><td>${Math.round(task.progressPercentage || 0)}%</td></tr>
                        <tr><td>总文档数:</td><td>${this.formatNumber(task.totalDocuments?.value || 0)}</td></tr>
                        <tr><td>已处理:</td><td>${this.formatNumber(task.processedDocuments?.value || 0)}</td></tr>
                        <tr><td>成功:</td><td>${this.formatNumber(task.successDocuments?.value || 0)}</td></tr>
                        <tr><td>失败:</td><td>${this.formatNumber(task.failedDocuments?.value || 0)}</td></tr>
                        <tr><td>当前批次:</td><td>${task.currentBatch?.value || 0}</td></tr>
                        <tr><td>总批次:</td><td>${task.totalBatches?.value || 0}</td></tr>
                        <tr><td>当前阶段:</td><td>${task.currentPhase || '未知'}</td></tr>
                    </table>
                </div>
            </div>
            
            ${task.errorMessage ? `
                <div class="row mt-3">
                    <div class="col-12">
                        <h6>错误信息</h6>
                        <div class="alert alert-danger">${task.errorMessage}</div>
                    </div>
                </div>
            ` : ''}
            
            <div class="row mt-3">
                <div class="col-12">
                    <h6>配置信息</h6>
                    <pre class="bg-light p-3 rounded"><code>${JSON.stringify(task.migrationConfig || task.indexSyncConfig || {}, null, 2)}</code></pre>
                </div>
            </div>
        `;
    }
    
    // 任务控制方法
    async pauseTask(taskId) {
        await this.sendTaskAction(taskId, 'pause');
    }
    
    async resumeTask(taskId) {
        await this.sendTaskAction(taskId, 'resume');
    }
    
    async cancelTask(taskId) {
        if (confirm('确定要取消这个任务吗？')) {
            await this.sendTaskAction(taskId, 'cancel');
        }
    }
    
    async deleteTask(taskId) {
        if (confirm('确定要删除这个任务吗？')) {
            try {
                const response = await fetch(`/api/monitor/tasks/${taskId}`, {
                    method: 'DELETE'
                });
                if (response.ok) {
                    const data = await response.json();
                    if (data.success) {
                        this.addLog(`任务 ${taskId} 删除成功`, 'success');
                    } else {
                        this.addLog(`删除任务失败: ${data.message}`, 'error');
                    }
                }
            } catch (error) {
                console.error('删除任务失败:', error);
                this.addLog('删除任务失败: ' + error.message, 'error');
            }
        }
    }
    
    async sendTaskAction(taskId, action) {
        try {
            const response = await fetch(`/api/monitor/tasks/${taskId}/${action}`, {
                method: 'POST'
            });
            if (response.ok) {
                const data = await response.json();
                if (data.success) {
                    this.addLog(data.message, 'success');
                } else {
                    this.addLog(data.message, 'error');
                }
            }
        } catch (error) {
            console.error(`执行任务操作失败(${action}):`, error);
            this.addLog(`执行任务操作失败: ${error.message}`, 'error');
        }
    }
    
    // 创建任务
    async createTask() {
        try {
            const taskType = document.getElementById('taskType').value;
            
            // 输入验证
            const validationResult = this.validateTaskInputs(taskType);
            if (!validationResult.valid) {
                ErrorHandler.showToast(validationResult.message, 'error');
                return;
            }
            
            let config = {};
            let url = '';
            
            if (taskType === 'FULL_MIGRATION' || taskType === 'INCREMENTAL_MIGRATION') {
                const sourceIndex = this.sanitizeInput(document.getElementById('sourceIndex').value);
                const targetIndex = this.sanitizeInput(document.getElementById('targetIndex').value);
                const batchSize = parseInt(document.getElementById('batchSize').value);
                const threadCount = parseInt(document.getElementById('threadCount').value);
                
                config = {
                    sourceIndex: sourceIndex,
                    targetIndex: targetIndex,
                    batchSize: Math.min(Math.max(batchSize, 100), 10000), // 限制范围
                    threadCount: Math.min(Math.max(threadCount, 1), 10), // 限制范围
                    scrollTimeout: 5,
                    overwriteExisting: true
                };
                url = taskType === 'FULL_MIGRATION' ? '/api/monitor/tasks/migration/full' : '/api/monitor/tasks/migration/incremental';
            } else if (taskType === 'INDEX_SYNC') {
                const indexNames = this.sanitizeInput(document.getElementById('indexNames').value);
                const batchSize = parseInt(document.getElementById('batchSize').value);
                const threadCount = parseInt(document.getElementById('threadCount').value);
                
                config = {
                    indexNames: indexNames ? indexNames.split(',').map(s => this.sanitizeInput(s.trim())).filter(s => s) : null,
                    batchSize: Math.min(Math.max(batchSize, 100), 10000),
                    threadCount: Math.min(Math.max(threadCount, 1), 10),
                    syncData: document.getElementById('syncData').checked,
                    syncMappings: document.getElementById('syncMappings').checked,
                    syncSettings: document.getElementById('syncSettings').checked,
                    syncAliases: true,
                    validateConnection: true,
                    validateData: true
                };
                url = '/api/monitor/tasks/sync';
            }
        
        try {
            const response = await fetch(url, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(config)
            });
            
            if (response.ok) {
                const data = await response.json();
                if (data.success) {
                    this.addLog(`任务创建成功: ${data.taskId}`, 'success');
                    const modal = bootstrap.Modal.getInstance(document.getElementById('createTaskModal'));
                    modal.hide();
                    // 重置表单
                    document.getElementById('createTaskForm').reset();
                } else {
                    this.addLog(`创建任务失败: ${data.message}`, 'error');
                }
            }
        } catch (error) {
            console.error('创建任务失败:', error);
            this.addLog('创建任务失败: ' + error.message, 'error');
        }
    }
    
    // 输入验证方法
    validateTaskInputs(taskType) {
        if (taskType === 'FULL_MIGRATION' || taskType === 'INCREMENTAL_MIGRATION') {
            const sourceIndex = document.getElementById('sourceIndex').value.trim();
            const targetIndex = document.getElementById('targetIndex').value.trim();
            const batchSize = parseInt(document.getElementById('batchSize').value);
            const threadCount = parseInt(document.getElementById('threadCount').value);
            
            if (!sourceIndex) {
                return { valid: false, message: '源索引名称不能为空' };
            }
            if (!targetIndex) {
                return { valid: false, message: '目标索引名称不能为空' };
            }
            if (sourceIndex === targetIndex) {
                return { valid: false, message: '源索引和目标索引不能相同' };
            }
            if (!this.isValidIndexName(sourceIndex)) {
                return { valid: false, message: '源索引名称格式不正确' };
            }
            if (!this.isValidIndexName(targetIndex)) {
                return { valid: false, message: '目标索引名称格式不正确' };
            }
            if (isNaN(batchSize) || batchSize < 100 || batchSize > 10000) {
                return { valid: false, message: '批次大小必须在100-10000之间' };
            }
            if (isNaN(threadCount) || threadCount < 1 || threadCount > 10) {
                return { valid: false, message: '线程数必须在1-10之间' };
            }
        }
        
        return { valid: true };
    }
    
    // 输入清理方法
    sanitizeInput(input) {
        if (typeof input !== 'string') return '';
        
        // 移除危险字符
        return input
            .replace(/[<>\"']/g, '') // 防XSS
            .replace(/[;|&$`\\]/g, '') // 防命令注入
            .replace(/\s+/g, ' ') // 标准化空格
            .trim()
            .substring(0, 100); // 限制长度
    }
    
    // 验证索引名称格式
    isValidIndexName(indexName) {
        // ES索引名称规则：小写字母、数字、- 和 _
        const pattern = /^[a-z0-9][a-z0-9_-]*$/;
        return pattern.test(indexName) && indexName.length <= 255;
    }
    
    // 工具方法
    getStatusIcon(status) {
        const icons = {
            PENDING: 'fas fa-clock',
            RUNNING: 'fas fa-spinner fa-spin',
            COMPLETED: 'fas fa-check',
            FAILED: 'fas fa-times',
            CANCELLED: 'fas fa-ban',
            PAUSED: 'fas fa-pause'
        };
        return `<i class="${icons[status] || 'fas fa-question'}"></i>`;
    }
    
    getStatusColor(status) {
        const colors = {
            PENDING: 'warning',
            RUNNING: 'primary',
            COMPLETED: 'success',
            FAILED: 'danger',
            CANCELLED: 'secondary',
            PAUSED: 'info'
        };
        return colors[status] || 'secondary';
    }
    
    formatNumber(num) {
        return new Intl.NumberFormat().format(num);
    }
    
    formatDuration(ms) {
        if (ms < 1000) return '< 1秒';
        const seconds = Math.floor(ms / 1000);
        const minutes = Math.floor(seconds / 60);
        const hours = Math.floor(minutes / 60);
        
        if (hours > 0) {
            return `${hours}小时${minutes % 60}分钟`;
        } else if (minutes > 0) {
            return `${minutes}分钟${seconds % 60}秒`;
        } else {
            return `${seconds}秒`;
        }
    }
    
    addLog(message, type = 'info') {
        const logs = document.getElementById('realTimeLogs');
        const logEntry = document.createElement('div');
        const timestamp = new Date().toLocaleTimeString();
        
        const colors = {
            info: 'text-info',
            success: 'text-success',
            warning: 'text-warning',
            error: 'text-danger'
        };
        
        logEntry.className = `mb-1 ${colors[type] || 'text-info'}`;
        logEntry.innerHTML = `<small>[${timestamp}]</small> ${message}`;
        
        logs.appendChild(logEntry);
        logs.scrollTop = logs.scrollHeight;
        
        // 限制日志数量
        if (logs.children.length > 100) {
            logs.removeChild(logs.firstChild);
        }
    }
    
    clearLogs() {
        document.getElementById('realTimeLogs').innerHTML = '';
    }
    
    // 刷新任务列表
    async refreshTasks() {
        await this.loadInitialData();
        this.addLog('任务列表已刷新', 'info');
    }
    
    // 智能自动刷新
    startAutoRefresh() {
        if (this.refreshInterval) {
            clearInterval(this.refreshInterval);
        }
        
        let refreshCount = 0;
        const maxRefreshes = 20; // 限制最大刷新次数，防止无限循环
        
        this.refreshInterval = setInterval(() => {
            if (!this.autoRefreshEnabled) return;
            
            // 如果WebSocket已连接，不需要轮询
            if (this.isConnected) return;
            
            // 防止页面隐藏时继续刷新
            if (document.hidden) return;
            
            refreshCount++;
            if (refreshCount > maxRefreshes) {
                console.warn('达到最大刷新次数，停止自动刷新');
                this.pauseUpdates();
                this.addLog('自动刷新已暂停，请手动刷新或检查网络连接', 'warning');
                return;
            }
            
            // 只刷新必要的数据，减少服务器压力
            this.loadEssentialData();
        }, 30000); // 30秒
    }
    
    // 加载关键数据（轻量版）
    async loadEssentialData() {
        try {
            // 只加载任务列表，不加载详细统计
            const tasksResponse = await ApiClient.get('/api/monitor/tasks', '轮询模式获取任务列表');
            if (tasksResponse.success) {
                this.updateTaskList(tasksResponse.tasks);
            }
        } catch (error) {
            console.error('轮询模式加载数据失败:', error);
            // 降级处理：延长刷新间隔
            this.degradeRefreshInterval();
        }
    }
    
    // 降级刷新间隔
    degradeRefreshInterval() {
        if (this.refreshInterval) {
            clearInterval(this.refreshInterval);
            console.log('网络异常，延长刷新间隔至60秒');
            this.refreshInterval = setInterval(() => {
                if (this.autoRefreshEnabled && !this.isConnected && !document.hidden) {
                    this.loadEssentialData();
                }
            }, 60000); // 降级到60秒
        }
    }
    
    // 绑定事件
    bindEvents() {
        document.getElementById('autoRefresh').addEventListener('change', (e) => {
            this.autoRefreshEnabled = e.target.checked;
        });
        
        // 页面可见性变化处理
        document.addEventListener('visibilitychange', () => {
            if (document.hidden) {
                this.pauseUpdates();
            } else {
                this.resumeUpdates();
            }
        });
        
        // 页面卸载时清理资源
        window.addEventListener('beforeunload', () => {
            this.cleanup();
        });
        
        // 页面错误处理
        window.addEventListener('error', (event) => {
            console.error('页面JavaScript错误:', event.error);
            this.addLog('页面发生错误，建议刷新页面', 'error');
        });
    }
    
    // 暂停更新
    pauseUpdates() {
        this.autoRefreshEnabled = false;
        const checkbox = document.getElementById('autoRefresh');
        if (checkbox) checkbox.checked = false;
        console.log('更新已暂停');
    }
    
    // 恢复更新
    resumeUpdates() {
        this.autoRefreshEnabled = true;
        const checkbox = document.getElementById('autoRefresh');
        if (checkbox) checkbox.checked = true;
        // 立即刷新一次数据
        this.loadInitialData();
        console.log('更新已恢复');
    }
    
    // 清理资源
    cleanup() {
        if (this.refreshInterval) {
            clearInterval(this.refreshInterval);
            this.refreshInterval = null;
        }
        if (this.stompClient && this.stompClient.connected) {
            this.stompClient.disconnect();
            this.stompClient = null;
        }
        this.isConnected = false;
        console.log('监控器资源已清理');
    }
    
    // 绑定页面卸载事件
    bindPageUnloadEvents() {
        window.addEventListener('beforeunload', () => {
            this.cleanup();
        });
        
        document.addEventListener('visibilitychange', () => {
            if (document.hidden) {
                // 页面隐藏时减少更新频率
                this.pauseUpdates();
            } else {
                // 页面显示时恢复更新
                this.resumeUpdates();
            }
        });
    }
    
    // 智能重连处理
    handleReconnect() {
        if (this.reconnectAttempts < this.maxReconnectAttempts) {
            this.reconnectAttempts++;
            const delay = this.reconnectDelay * Math.pow(2, this.reconnectAttempts - 1); // 指数退避
            
            console.log(`WebSocket重连尝试 ${this.reconnectAttempts}/${this.maxReconnectAttempts}，${delay}ms后重试`);
            this.addLog(`WebSocket重连尝试 ${this.reconnectAttempts}/${this.maxReconnectAttempts}`, 'warning');
            
            setTimeout(() => {
                this.connectWebSocket();
            }, delay);
        } else {
            console.error('WebSocket重连失败，已达到最大重试次数');
            this.addLog('WebSocket连接彻底失败，请刷新页面重试', 'error');
            // 切换到轮询模式
            this.switchToPollingMode();
        }
    }
    
    // 切换到轮询模式
    switchToPollingMode() {
        this.addLog('切换到轮询模式，数据更新可能延迟', 'warning');
        this.startAutoRefresh();
    }
    
    // 暂停更新
    pauseUpdates() {
        if (this.refreshInterval) {
            clearInterval(this.refreshInterval);
            this.refreshInterval = null;
        }
    }
    
    // 恢复更新
    resumeUpdates() {
        if (!this.isConnected) {
            this.startAutoRefresh();
        }
    }
    
    // 清理资源
    cleanup() {
        if (this.stompClient && this.isConnected) {
            try {
                this.stompClient.disconnect();
                console.log('WebSocket连接已断开');
            } catch (error) {
                console.error('断开WebSocket连接时出错:', error);
            }
        }
        
        if (this.refreshInterval) {
            clearInterval(this.refreshInterval);
            this.refreshInterval = null;
        }
        
        this.isConnected = false;
    }
}

// 显示创建任务模态框
function showCreateTaskModal() {
    const modal = new bootstrap.Modal(document.getElementById('createTaskModal'));
    modal.show();
}

// 切换配置字段显示
function toggleConfigFields() {
    const taskType = document.getElementById('taskType').value;
    const migrationFields = document.getElementById('migrationFields');
    const syncFields = document.getElementById('syncFields');
    
    if (taskType === 'INDEX_SYNC') {
        migrationFields.style.display = 'none';
        syncFields.style.display = 'block';
    } else {
        migrationFields.style.display = 'block';
        syncFields.style.display = 'none';
    }
}

// 全局函数
function refreshTasks() {
    monitor.refreshTasks();
}

function clearLogs() {
    monitor.clearLogs();
}

function createTask() {
    monitor.createTask();
}

// 测试图表函数
function testCharts() {
    console.log('测试图表功能');
    try {
        // 测试内存圆环图 - 45%
        SimpleCharts.createMemoryDonutChart('memoryChart', 45);
        
        // 测试CPU仪表盘 - 30%
        SimpleCharts.createCpuGauge('cpuChart', 30);
        
        // 测试性能趋势图 - 多个数据点
        SimpleCharts.createPerformanceChart('performanceChart', [
            { label: '第1小时', value: 150 },
            { label: '第2小时', value: 200 },
            { label: '第3小时', value: 180 },
            { label: '第4小时', value: 220 },
            { label: '第5小时', value: 160 }
        ]);
        
        // 测试历史趋势图 - 带数据
        SimpleCharts.createTrendChart('trendChart', [
            { hour: 1, throughput: 100 },
            { hour: 2, throughput: 150 },
            { hour: 3, throughput: 120 },
            { hour: 4, throughput: 180 }
        ]);
        
        console.log('图表测试完成');
    } catch (error) {
        console.error('图表测试失败:', error);
    }
}

function showSystemStatsModal() {
    // 显示系统统计详情模态框
    const modal = new bootstrap.Modal(document.getElementById('systemStatsModal'));
    modal.show();
    
    // 加载详细统计数据
    loadDetailedSystemStats();
}

// 加载详细系统统计数据
async function loadDetailedSystemStats() {
    try {
        const response = await fetch('/api/monitor/stats/comprehensive');
        if (response.ok) {
            const data = await response.json();
            if (data.success) {
                renderSystemStatsModal(data.stats);
            } else {
                ErrorHandler.showError({ message: data.message }, '获取详细统计数据');
            }
        }
    } catch (error) {
        console.error('加载详细统计数据失败:', error);
        ErrorHandler.showError(error, '获取详细统计数据');
    }
}

// 渲染系统统计模态框内容
function renderSystemStatsModal(stats) {
    const content = document.getElementById('systemStatsModalContent');
    if (!content) return;
    
    content.innerHTML = `
        <div class="row">
            <!-- 任务统计 -->
            <div class="col-md-6 mb-3">
                <div class="card">
                    <div class="card-header">
                        <h6 class="mb-0"><i class="fas fa-tasks"></i> 任务统计</h6>
                    </div>
                    <div class="card-body">
                        <table class="table table-sm">
                            <tr><td>总任务数:</td><td><strong>${stats.taskStats?.totalTasks || 0}</strong></td></tr>
                            <tr><td>运行中:</td><td><span class="badge bg-primary">${stats.taskStats?.runningTasks || 0}</span></td></tr>
                            <tr><td>已完成:</td><td><span class="badge bg-success">${stats.taskStats?.completedTasks || 0}</span></td></tr>
                            <tr><td>失败:</td><td><span class="badge bg-danger">${stats.taskStats?.failedTasks || 0}</span></td></tr>
                            <tr><td>成功率:</td><td>${stats.taskStats?.successRate || '0.00%'}</td></tr>
                            <tr><td>完成率:</td><td>${stats.taskStats?.completionRate || '0.00%'}</td></tr>
                            <tr><td>平均时长:</td><td>${stats.taskStats?.averageTaskDuration || '暂无数据'}</td></tr>
                        </table>
                    </div>
                </div>
            </div>
            
            <!-- 性能统计 -->
            <div class="col-md-6 mb-3">
                <div class="card">
                    <div class="card-header">
                        <h6 class="mb-0"><i class="fas fa-tachometer-alt"></i> 性能统计</h6>
                    </div>
                    <div class="card-body">
                        <table class="table table-sm">
                            <tr><td>当前总速度:</td><td>${stats.performanceStats?.currentTotalSpeed || '0 文档/秒'}</td></tr>
                            <tr><td>平均速度:</td><td>${stats.performanceStats?.currentAverageSpeed || '0 文档/秒'}</td></tr>
                            <tr><td>整体吞吐量:</td><td>${stats.performanceStats?.overallThroughput || '暂无数据'}</td></tr>
                            <tr><td>预计完成:</td><td>${stats.performanceStats?.estimatedCompletionTime || '计算中...'}</td></tr>
                            <tr><td>总处理时间:</td><td>${stats.performanceStats?.totalProcessingTimeHours || '0.00'}小时</td></tr>
                            <tr><td>总处理文档:</td><td>${new Intl.NumberFormat().format(stats.performanceStats?.totalProcessedDocuments || 0)}</td></tr>
                        </table>
                    </div>
                </div>
            </div>
            
            <!-- 系统资源 -->
            <div class="col-md-6 mb-3">
                <div class="card">
                    <div class="card-header">
                        <h6 class="mb-0"><i class="fas fa-server"></i> 系统资源</h6>
                    </div>
                    <div class="card-body">
                        ${stats.systemResources?.memory ? `
                        <h6>内存使用</h6>
                        <div class="mb-2">
                            <div class="d-flex justify-content-between">
                                <small>堆内存</small>
                                <small>${stats.systemResources.memory.heapUsagePercent}</small>
                            </div>
                            <div class="progress mb-2">
                                <div class="progress-bar ${parseFloat(stats.systemResources.memory.heapUsagePercent?.replace('%', '') || '0') > 80 ? 'bg-danger' : parseFloat(stats.systemResources.memory.heapUsagePercent?.replace('%', '') || '0') > 60 ? 'bg-warning' : 'bg-success'}" 
                                     style="width: ${stats.systemResources.memory.heapUsagePercent}"></div>
                            </div>
                            <small>${stats.systemResources.memory.heapUsed}MB / ${stats.systemResources.memory.heapMax}MB</small>
                        </div>
                        ` : ''}
                        
                        ${stats.systemResources?.threads ? `
                        <h6>线程</h6>
                        <table class="table table-sm">
                            <tr><td>当前线程数:</td><td>${stats.systemResources.threads.currentThreadCount}</td></tr>
                            <tr><td>峰值线程数:</td><td>${stats.systemResources.threads.peakThreadCount}</td></tr>
                        </table>
                        ` : ''}
                        
                        ${stats.systemResources?.garbageCollection ? `
                        <h6>垃圾回收</h6>
                        <table class="table table-sm">
                            <tr><td>GC次数:</td><td>${stats.systemResources.garbageCollection.totalGcCount}</td></tr>
                            <tr><td>GC时间:</td><td>${stats.systemResources.garbageCollection.totalGcTime}</td></tr>
                        </table>
                        ` : ''}
                    </div>
                </div>
            </div>
            
            <!-- 实时指标 -->
            <div class="col-md-6 mb-3">
                <div class="card">
                    <div class="card-header">
                        <h6 class="mb-0"><i class="fas fa-chart-line"></i> 实时指标</h6>
                    </div>
                    <div class="card-body">
                        <table class="table table-sm">
                            <tr><td>系统负载:</td><td>${stats.realtimeMetrics?.systemLoadAverage || '不支持'}</td></tr>
                            <tr><td>服务器时间:</td><td>${stats.realtimeMetrics?.serverTime || '未知'}</td></tr>
                            <tr><td>系统运行时长:</td><td>${stats.realtimeMetrics?.uptime ? monitor.formatDuration(stats.realtimeMetrics.uptime * 1000) : '未知'}</td></tr>
                        </table>
                        
                        ${stats.realtimeMetrics?.activeTasks && Object.keys(stats.realtimeMetrics.activeTasks).length > 0 ? `
                        <h6>活跃任务</h6>
                        <div class="active-tasks-detail">
                            ${Object.keys(stats.realtimeMetrics.activeTasks).map(taskId => {
                                const task = stats.realtimeMetrics.activeTasks[taskId];
                                return `
                                    <div class="mb-2 p-2 border rounded">
                                        <div class="d-flex justify-content-between">
                                            <small><strong>${taskId}</strong></small>
                                            <small>${task.phase || '处理中'}</small>
                                        </div>
                                        <div class="progress mb-1" style="height: 4px;">
                                            <div class="progress-bar" style="width: ${task.progress || 0}%"></div>
                                        </div>
                                        <div class="d-flex justify-content-between">
                                            <small>速度: ${task.speed || 0} docs/s</small>
                                            <small>时长: ${monitor.formatDuration((task.duration || 0) * 1000)}</small>
                                        </div>
                                    </div>
                                `;
                            }).join('')}
                        </div>
                        ` : '<div class="text-muted">暂无活跃任务</div>'}
                    </div>
                </div>
            </div>
            
            <!-- 异常统计 -->
            ${stats.exceptionStats ? `
            <div class="col-md-6 mb-3">
                <div class="card">
                    <div class="card-header">
                        <h6 class="mb-0"><i class="fas fa-exclamation-triangle"></i> 异常统计</h6>
                    </div>
                    <div class="card-body">
                        <table class="table table-sm">
                            <tr><td>总异常数:</td><td><span class="badge bg-danger">${stats.exceptionStats.totalExceptions || 0}</span></td></tr>
                            <tr><td>最近异常数:</td><td><span class="badge bg-warning">${stats.exceptionStats.recentExceptions || 0}</span></td></tr>
                        </table>
                    </div>
                </div>
            </div>
            ` : ''}
            
            <!-- 趋势数据 -->
            ${stats.trendData ? `
            <div class="col-md-6 mb-3">
                <div class="card">
                    <div class="card-header">
                        <h6 class="mb-0"><i class="fas fa-chart-area"></i> 趋势数据</h6>
                    </div>
                    <div class="card-body">
                        <table class="table table-sm">
                            <tr><td>数据点数量:</td><td>${stats.trendData.dataPoints || 0}</td></tr>
                        </table>
                        <div class="text-muted">
                            <small>24小时历史趋势数据 (${stats.trendData.dataPoints || 0} 个数据点)</small>
                        </div>
                    </div>
                </div>
            </div>
            ` : ''}
        </div>
        
        <div class="row mt-3">
            <div class="col-12">
                <div class="alert alert-info">
                    <small>
                        <i class="fas fa-info-circle"></i> 
                        统计数据生成时间: ${stats.generatedAt || '未知'} | 
                        数据时间戳: ${new Date(stats.timestamp || Date.now()).toLocaleString()}
                    </small>
                </div>
            </div>
        </div>
    `;
}

// ===========================
// 轻量级图表组件
// ===========================

class SimpleCharts {
    // 创建性能趋势图
    static createPerformanceChart(containerId, data = []) {
        const container = document.getElementById(containerId);
        if (!container) {
            console.error('性能图表容器未找到:', containerId);
            return;
        }
        
        console.log(`创建性能趋势图: ${data.length} 个数据点`, data);
        
        // 如果没有数据，显示占位符
        if (!data || data.length === 0) {
            container.innerHTML = `
                <div class="text-center text-muted py-3">
                    <i class="fas fa-chart-line fa-2x mb-2"></i>
                    <br>暂无性能数据
                </div>
            `;
            return;
        }
        
        // 创建简单的条形图
        const maxValue = Math.max(...data.map(d => d.value || 0));
        console.log('性能图表最大值:', maxValue);
        
        // 如果所有值都是0，显示暂无数据
        if (maxValue === 0) {
            container.innerHTML = `
                <div class="text-center text-muted py-3">
                    <i class="fas fa-chart-line fa-2x mb-2"></i>
                    <br>暂无性能数据
                </div>
            `;
            return;
        }
        
        const bars = data.slice(-10).map((item, index) => {
            const value = item.value || 0;
            const height = Math.max(2, (value / maxValue) * 50); // 最小高度2px
            
            console.log(`条形图 ${index}: 值=${value}, 高度=${height}px`);
            
            return `
                <div class="chart-bar" style="
                    display: inline-block;
                    width: ${80 / data.slice(-10).length}%;
                    height: ${height}px;
                    background: linear-gradient(to top, #007bff, #0056b3);
                    margin: 0 1px;
                    border-radius: 2px 2px 0 0;
                    vertical-align: bottom;
                    position: relative;
                " title="${item.label || ''}: ${item.value || 0}">
                </div>
            `;
        }).join('');
        
        container.innerHTML = `
            <div style="height: 60px; display: flex; align-items: end; justify-content: center; border-bottom: 1px solid #dee2e6; margin-bottom: 5px;">
                ${bars}
            </div>
            <small class="text-muted">处理速度趋势</small>
        `;
    }
    
    // 创建内存使用圆环图
    static createMemoryDonutChart(containerId, usedPercent = 0) {
        const container = document.getElementById(containerId);
        if (!container) {
            console.error('内存图表容器未找到:', containerId);
            return;
        }
        
        const percentage = Math.min(Math.max(usedPercent, 0), 100);
        const color = percentage > 80 ? '#dc3545' : percentage > 60 ? '#ffc107' : '#28a745';
        const circumference = 2 * Math.PI * 30;
        const offset = circumference * (1 - percentage / 100);
        
        console.log(`创建内存圆环图: ${percentage}%, 颜色: ${color}`);
        
        container.innerHTML = `
            <div style="position: relative; width: 80px; height: 80px; margin: 0 auto;">
                <svg width="80" height="80" style="transform: rotate(-90deg);">
                    <!-- 背景圆圈 -->
                    <circle cx="40" cy="40" r="30" stroke="#e9ecef" stroke-width="8" fill="none"></circle>
                    <!-- 进度圆圈 -->
                    <circle cx="40" cy="40" r="30" stroke="${color}" stroke-width="8" fill="none"
                            stroke-dasharray="${circumference}"
                            stroke-dashoffset="${offset}"
                            style="transition: stroke-dashoffset 0.5s ease;">
                    </circle>
                </svg>
                <div style="
                    position: absolute;
                    top: 50%;
                    left: 50%;
                    transform: translate(-50%, -50%);
                    font-size: 12px;
                    font-weight: bold;
                    color: ${color};
                ">${percentage.toFixed(1)}%</div>
            </div>
            <small class="text-muted d-block mt-2">内存使用率</small>
        `;
    }
    
    // 创建CPU使用仪表盘
    static createCpuGauge(containerId, cpuPercent = 0) {
        const container = document.getElementById(containerId);
        if (!container) {
            console.error('CPU图表容器未找到:', containerId);
            return;
        }
        
        const percentage = Math.min(Math.max(cpuPercent, 0), 100);
        const color = percentage > 80 ? '#dc3545' : percentage > 60 ? '#ffc107' : '#28a745';
        const arcLength = Math.PI * 30; // 半圆弧长
        const offset = arcLength * (1 - percentage / 100);
        
        console.log(`创建CPU仪表盘: ${percentage}%, 颜色: ${color}`);
        
        container.innerHTML = `
            <div style="position: relative; width: 80px; height: 50px; margin: 0 auto;">
                <svg width="80" height="50">
                    <!-- 背景半圆 -->
                    <path d="M 10 40 A 30 30 0 0 1 70 40" stroke="#e9ecef" stroke-width="6" fill="none"></path>
                    <!-- 进度半圆 -->
                    <path d="M 10 40 A 30 30 0 0 1 70 40" stroke="${color}" stroke-width="6" fill="none"
                          stroke-dasharray="${arcLength}"
                          stroke-dashoffset="${offset}"
                          style="transition: stroke-dashoffset 0.5s ease;"></path>
                </svg>
                <div style="
                    position: absolute;
                    bottom: 5px;
                    left: 50%;
                    transform: translateX(-50%);
                    font-size: 10px;
                    font-weight: bold;
                    color: ${color};
                ">${percentage.toFixed(1)}%</div>
            </div>
            <small class="text-muted d-block mt-1">CPU使用率</small>
        `;
    }
    
    // 创建趋势柱状图
    static createTrendChart(containerId, hourlyData = []) {
        const container = document.getElementById(containerId);
        if (!container) {
            console.error('趋势图表容器未找到:', containerId);
            return;
        }
        
        console.log(`创建趋势图: ${hourlyData.length} 个数据点`);
        
        if (!hourlyData || hourlyData.length === 0) {
            container.innerHTML = `
                <div class="text-center text-muted py-2">
                    <i class="fas fa-chart-bar"></i>
                    <br><small>暂无趋势数据</small>
                </div>
            `;
            return;
        }
        
        const maxValue = Math.max(...hourlyData.map(d => d.throughput || 0));
        const bars = hourlyData.slice(-12).map((item, index) => {
            const height = maxValue > 0 ? (item.throughput / maxValue) * 40 : 0;
            return `
                <div style="
                    display: inline-block;
                    width: ${80 / hourlyData.slice(-12).length}%;
                    height: ${height}px;
                    background: linear-gradient(to top, #17a2b8, #138496);
                    margin: 0 1px;
                    border-radius: 1px 1px 0 0;
                    vertical-align: bottom;
                " title="第${item.hour}小时: ${item.throughput || 0} 文档/秒">
                </div>
            `;
        }).join('');
        
        container.innerHTML = `
            <div style="height: 40px; display: flex; align-items: end; justify-content: center; margin-bottom: 5px;">
                ${bars}
            </div>
            <small class="text-muted">24小时趋势</small>
        `;
    }
}

// 扩展MigrationMonitor类，添加图表初始化方法
MigrationMonitor.prototype.initializeCharts = function() {
    console.log('开始初始化图表...');
    
    // 检查所有图表容器是否存在
    const containers = [
        'performanceChart', 'memoryChart', 'cpuChart', 'trendChart'
    ];
    
    containers.forEach(id => {
        const element = document.getElementById(id);
        console.log(`容器 ${id}:`, element ? '找到' : '未找到');
    });
    
    try {
        // 初始化性能趋势图（空状态）
        console.log('初始化性能趋势图...');
        SimpleCharts.createPerformanceChart('performanceChart', []);
        
        // 初始化内存使用圆环图（0%状态）
        console.log('初始化内存使用圆环图...');
        SimpleCharts.createMemoryDonutChart('memoryChart', 0);
        
        // 初始化CPU使用仪表盘（0%状态）
        console.log('初始化CPU使用仪表盘...');
        SimpleCharts.createCpuGauge('cpuChart', 0);
        
        // 初始化历史趋势图（空状态）
        console.log('初始化历史趋势图...');
        SimpleCharts.createTrendChart('trendChart', []);
        
        console.log('图表初始化完成');
    } catch (error) {
        console.error('图表初始化失败:', error);
        console.error('错误堆栈:', error.stack);
    }
};

// 扩展MigrationMonitor类，添加图表更新方法
MigrationMonitor.prototype.updateCharts = function(statsData) {
    console.log('更新图表，数据:', statsData);
    
    // 更新性能趋势图
    if (statsData.performanceStats) {
        const performanceData = [
            { label: '当前速度', value: parseInt(statsData.performanceStats.currentTotalSpeed?.replace(/[^\d]/g, '') || 0) },
            { label: '平均速度', value: parseInt(statsData.performanceStats.currentAverageSpeed?.replace(/[^\d]/g, '') || 0) }
        ];
        SimpleCharts.createPerformanceChart('performanceChart', performanceData);
    } else {
        SimpleCharts.createPerformanceChart('performanceChart', []);
    }
    
    // 更新内存使用圆环图
    let memoryPercent = 0;
    if (statsData.systemResources?.memory?.heapUsagePercent) {
        memoryPercent = parseFloat(statsData.systemResources.memory.heapUsagePercent.replace('%', '') || 0);
        console.log('内存使用率:', memoryPercent);
    } else {
        console.log('没有内存数据，使用默认值');
    }
    SimpleCharts.createMemoryDonutChart('memoryChart', memoryPercent);
    
    // 更新CPU使用仪表盘（模拟数据，因为Java中SystemLoad可能不支持）
    const systemLoad = statsData.realtimeMetrics?.systemLoadAverage;
    let cpuPercent = 0;
    if (systemLoad && systemLoad !== '不支持' && !isNaN(parseFloat(systemLoad))) {
        cpuPercent = Math.min(parseFloat(systemLoad) * 25, 100); // 简单转换
    }
    SimpleCharts.createCpuGauge('cpuChart', cpuPercent);
    
    // 更新趋势图
    if (statsData.trendData?.hourlyTrends) {
        SimpleCharts.createTrendChart('trendChart', statsData.trendData.hourlyTrends);
    } else {
        SimpleCharts.createTrendChart('trendChart', []);
    }
};

// 扩展原有的更新方法
const originalUpdateEnhancedSystemStats = MigrationMonitor.prototype.updateEnhancedSystemStats;
MigrationMonitor.prototype.updateEnhancedSystemStats = function(message) {
    // 调用原始方法
    originalUpdateEnhancedSystemStats.call(this, message);
    
    // 更新图表
    this.updateCharts(message);
};

const originalUpdateComprehensiveStatsDisplay = MigrationMonitor.prototype.updateComprehensiveStatsDisplay;
MigrationMonitor.prototype.updateComprehensiveStatsDisplay = function(stats) {
    // 调用原始方法
    if (originalUpdateComprehensiveStatsDisplay) {
        originalUpdateComprehensiveStatsDisplay.call(this, stats);
    }
    
    // 更新图表
    this.updateCharts(stats);
};

// 初始化监控器
const monitor = new MigrationMonitor();