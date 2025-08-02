const { scrapeXSMB } = require('../../scraper');
const fs = require('fs');
const path = require('path');

// Cấu hình timezone cho Việt Nam
process.env.TZ = 'Asia/Ho_Chi_Minh';

class ScraperScheduler {
    constructor() {
        this.isRunning = false;
        this.lastRunDate = null;
        this.schedulerInterval = null;
        this.lockFile = path.join(__dirname, '../../scraper.lock');
        this.statusFile = path.join(__dirname, '../../scraper.status.json');

        // ✅ TỐI ƯU: Cache để giảm I/O operations
        this.statusCache = null;
        this.lastStatusUpdate = 0;
        this.statusCacheDuration = 30 * 1000; // 30 giây cache

        // ✅ TỐI ƯU: Debounce để tránh spam logs
        this.lastLogTime = 0;
        this.logDebounceTime = 5000; // 5 giây

        // ✅ TỐI ƯU: Performance monitoring
        this.performanceStats = {
            totalChecks: 0,
            successfulRuns: 0,
            failedRuns: 0,
            averageCheckTime: 0,
            lastCheckTime: 0
        };

        this.initializeScheduler();
    }

    // Khởi tạo scheduler
    initializeScheduler() {
        console.log('🚀 Khởi tạo Scraper Scheduler cho XSMB...');

        // Kiểm tra và tạo file status nếu chưa có
        this.ensureStatusFile();

        // Bắt đầu scheduler
        this.startScheduler();

        // Xử lý graceful shutdown
        this.handleGracefulShutdown();
    }

    // Đảm bảo file status tồn tại
    ensureStatusFile() {
        if (!fs.existsSync(this.statusFile)) {
            const initialStatus = {
                lastRun: null,
                nextRun: this.calculateNextRun(),
                isRunning: false,
                lastError: null,
                totalRuns: 0,
                successfulRuns: 0,
                failedRuns: 0,
                performanceStats: this.performanceStats
            };
            fs.writeFileSync(this.statusFile, JSON.stringify(initialStatus, null, 2));
        }
    }

    // Tính toán thời gian chạy tiếp theo (18h14)
    calculateNextRun() {
        const now = new Date();
        const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
        const targetTime = new Date(today.getTime() + (18 * 60 + 14) * 60 * 1000); // 18h14

        // Nếu đã qua 18h14 hôm nay, lên lịch cho ngày mai
        if (now >= targetTime) {
            targetTime.setDate(targetTime.getDate() + 1);
        }

        return targetTime.toISOString();
    }

    // ✅ TỐI ƯU: Bắt đầu scheduler với interval thông minh
    startScheduler() {
        if (this.schedulerInterval) {
            clearInterval(this.schedulerInterval);
        }

        // ✅ TỐI ƯU: Kiểm tra thông minh - chỉ check vào phút 14
        const now = new Date();
        const currentMinute = now.getMinutes();
        const currentSecond = now.getSeconds();

        // Tính thời gian đến phút tiếp theo có thể chạy
        let initialDelay = 60 * 1000; // Mặc định 1 phút

        if (currentMinute === 13) {
            // Nếu đang ở phút 13, chờ đến phút 14
            initialDelay = (60 - currentSecond) * 1000;
        } else if (currentMinute === 14) {
            // Nếu đang ở phút 14, chạy ngay
            initialDelay = 0;
        } else {
            // Tính thời gian đến phút 14 tiếp theo
            const minutesToNext = (14 - currentMinute + 60) % 60;
            initialDelay = (minutesToNext * 60 - currentSecond) * 1000;
        }

        // ✅ TỐI ƯU: Chạy ngay lập tức nếu cần
        if (initialDelay === 0) {
            this.checkAndRun();
        }

        // ✅ TỐI ƯU: Interval thông minh - chỉ check vào phút 14
        this.schedulerInterval = setInterval(() => {
            this.checkAndRun();
        }, 60 * 1000); // 1 phút

        console.log(`✅ Scheduler đã khởi động, kiểm tra thông minh (delay: ${Math.round(initialDelay / 1000)}s)`);
        this.logStatus();
    }

    // ✅ TỐI ƯU: Kiểm tra và chạy scraper với performance monitoring
    async checkAndRun() {
        const startTime = Date.now();
        this.performanceStats.totalChecks++;

        try {
            const now = new Date();
            const currentHour = now.getHours();
            const currentMinute = now.getMinutes();

            // ✅ TỐI ƯU: Chỉ check vào phút 14 để tiết kiệm CPU
            if (currentHour === 18 && currentMinute === 14) {
                // Kiểm tra xem đã chạy hôm nay chưa
                const today = now.toDateString();
                if (this.lastRunDate === today) {
                    this.logWithDebounce('⏰ Scraper đã chạy hôm nay, bỏ qua');
                    return;
                }

                // ✅ TỐI ƯU: Kiểm tra lock file với timeout
                if (await this.isLockedWithTimeout()) {
                    this.logWithDebounce('🔒 Scraper đang chạy, bỏ qua');
                    return;
                }

                await this.runScraper();
            }
        } catch (error) {
            console.error('❌ Lỗi trong scheduler:', error.message);
            this.updateStatus({ lastError: error.message });
            this.performanceStats.failedRuns++;
        } finally {
            // ✅ TỐI ƯU: Cập nhật performance stats
            const checkTime = Date.now() - startTime;
            this.performanceStats.lastCheckTime = checkTime;
            this.performanceStats.averageCheckTime =
                (this.performanceStats.averageCheckTime * (this.performanceStats.totalChecks - 1) + checkTime) / this.performanceStats.totalChecks;
        }
    }

    // ✅ TỐI ƯU: Log với debounce để tránh spam
    logWithDebounce(message) {
        const now = Date.now();
        if (now - this.lastLogTime > this.logDebounceTime) {
            console.log(message);
            this.lastLogTime = now;
        }
    }

    // ✅ TỐI ƯU: Kiểm tra lock với timeout
    async isLockedWithTimeout() {
        return new Promise((resolve) => {
            const timeout = setTimeout(() => {
                resolve(false); // Timeout, coi như không locked
            }, 1000); // 1 giây timeout

            try {
                if (fs.existsSync(this.lockFile)) {
                    const lockData = JSON.parse(fs.readFileSync(this.lockFile, 'utf8'));
                    const lockTime = new Date(lockData.timestamp);
                    const now = new Date();

                    // Lock hết hạn sau 30 phút
                    if (now.getTime() - lockTime.getTime() > 30 * 60 * 1000) {
                        this.removeLock();
                        clearTimeout(timeout);
                        resolve(false);
                        return;
                    }
                    clearTimeout(timeout);
                    resolve(true);
                    return;
                }
                clearTimeout(timeout);
                resolve(false);
            } catch (error) {
                console.error('Lỗi kiểm tra lock file:', error.message);
                clearTimeout(timeout);
                resolve(false);
            }
        });
    }

    // Kiểm tra lock file (legacy - giữ lại cho compatibility)
    isLocked() {
        try {
            if (fs.existsSync(this.lockFile)) {
                const lockData = JSON.parse(fs.readFileSync(this.lockFile, 'utf8'));
                const lockTime = new Date(lockData.timestamp);
                const now = new Date();

                // Lock hết hạn sau 30 phút
                if (now.getTime() - lockTime.getTime() > 30 * 60 * 1000) {
                    this.removeLock();
                    return false;
                }
                return true;
            }
            return false;
        } catch (error) {
            console.error('Lỗi kiểm tra lock file:', error.message);
            return false;
        }
    }

    // Tạo lock file
    createLock() {
        try {
            const lockData = {
                timestamp: new Date().toISOString(),
                pid: process.pid
            };
            fs.writeFileSync(this.lockFile, JSON.stringify(lockData, null, 2));
        } catch (error) {
            console.error('Lỗi tạo lock file:', error.message);
        }
    }

    // Xóa lock file
    removeLock() {
        try {
            if (fs.existsSync(this.lockFile)) {
                fs.unlinkSync(this.lockFile);
            }
        } catch (error) {
            console.error('Lỗi xóa lock file:', error.message);
        }
    }

    // ✅ TỐI ƯU: Chạy scraper với error handling tốt hơn
    async runScraper() {
        const startTime = Date.now();

        try {
            console.log('🎯 Bắt đầu chạy scraper XSMB tự động...');

            this.createLock();
            this.isRunning = true;
            this.updateStatus({ isRunning: true, lastRun: new Date().toISOString() });

            // Lấy ngày hiện tại
            const today = new Date();
            const date = today.toLocaleDateString('vi-VN', {
                day: '2-digit',
                month: '2-digit',
                year: 'numeric'
            }).split('/').join('/');

            console.log(`📅 Chạy scraper cho ngày: ${date}, đài: xsmb`);

            // ✅ TỐI ƯU: Chạy scraper với timeout
            const scraperPromise = scrapeXSMB(date, 'xsmb');
            const timeoutPromise = new Promise((_, reject) => {
                setTimeout(() => reject(new Error('Scraper timeout after 20 minutes')), 20 * 60 * 1000);
            });

            await Promise.race([scraperPromise, timeoutPromise]);

            // Cập nhật trạng thái thành công
            this.lastRunDate = today.toDateString();
            this.performanceStats.successfulRuns++;

            this.updateStatus({
                isRunning: false,
                totalRuns: this.getStatus().totalRuns + 1,
                successfulRuns: this.performanceStats.successfulRuns,
                failedRuns: this.performanceStats.failedRuns,
                lastError: null,
                performanceStats: this.performanceStats
            });

            const duration = Date.now() - startTime;
            console.log(`✅ Scraper XSMB hoàn thành thành công trong ${Math.round(duration / 1000)}s`);

        } catch (error) {
            console.error('❌ Lỗi khi chạy scraper:', error.message);
            this.performanceStats.failedRuns++;

            this.updateStatus({
                isRunning: false,
                totalRuns: this.getStatus().totalRuns + 1,
                successfulRuns: this.performanceStats.successfulRuns,
                failedRuns: this.performanceStats.failedRuns,
                lastError: error.message,
                performanceStats: this.performanceStats
            });
        } finally {
            this.removeLock();
            this.isRunning = false;
        }
    }

    // ✅ TỐI ƯU: Lấy trạng thái với cache
    getStatus() {
        const now = Date.now();

        // Sử dụng cache nếu còn valid
        if (this.statusCache && (now - this.lastStatusUpdate) < this.statusCacheDuration) {
            return this.statusCache;
        }

        try {
            if (fs.existsSync(this.statusFile)) {
                const status = JSON.parse(fs.readFileSync(this.statusFile, 'utf8'));
                // ✅ TỐI ƯU: Cập nhật cache
                this.statusCache = status;
                this.lastStatusUpdate = now;
                return status;
            }
            return {
                lastRun: null,
                nextRun: this.calculateNextRun(),
                isRunning: false,
                lastError: null,
                totalRuns: 0,
                successfulRuns: 0,
                failedRuns: 0,
                performanceStats: this.performanceStats
            };
        } catch (error) {
            console.error('Lỗi đọc status file:', error.message);
            return {
                lastRun: null,
                nextRun: this.calculateNextRun(),
                isRunning: false,
                lastError: null,
                totalRuns: 0,
                successfulRuns: 0,
                failedRuns: 0,
                performanceStats: this.performanceStats
            };
        }
    }

    // ✅ TỐI ƯU: Cập nhật trạng thái với cache invalidation
    updateStatus(updates) {
        try {
            const status = this.getStatus();
            const updatedStatus = { ...status, ...updates };
            fs.writeFileSync(this.statusFile, JSON.stringify(updatedStatus, null, 2));

            // ✅ TỐI ƯU: Invalidate cache
            this.statusCache = null;
            this.lastStatusUpdate = 0;
        } catch (error) {
            console.error('Lỗi cập nhật status file:', error.message);
        }
    }

    // ✅ TỐI ƯU: Log trạng thái với performance stats
    logStatus() {
        const status = this.getStatus();
        const nextRun = new Date(status.nextRun);
        console.log('📊 Trạng thái Scheduler:');
        console.log(`   - Lần chạy cuối: ${status.lastRun ? new Date(status.lastRun).toLocaleString('vi-VN') : 'Chưa có'}`);
        console.log(`   - Lần chạy tiếp theo: ${nextRun.toLocaleString('vi-VN')}`);
        console.log(`   - Đang chạy: ${status.isRunning ? 'Có' : 'Không'}`);
        console.log(`   - Tổng lần chạy: ${status.totalRuns}`);
        console.log(`   - Thành công: ${status.successfulRuns}`);
        console.log(`   - Thất bại: ${status.failedRuns}`);
        console.log(`   - Performance: ${Math.round(this.performanceStats.averageCheckTime)}ms/check`);
        if (status.lastError) {
            console.log(`   - Lỗi cuối: ${status.lastError}`);
        }
    }

    // Xử lý graceful shutdown
    handleGracefulShutdown() {
        const cleanup = () => {
            console.log('🛑 Đang dừng Scheduler...');
            if (this.schedulerInterval) {
                clearInterval(this.schedulerInterval);
            }
            this.removeLock();
            this.updateStatus({ isRunning: false });
            console.log('✅ Scheduler đã dừng an toàn');
            process.exit(0);
        };

        process.on('SIGINT', cleanup);
        process.on('SIGTERM', cleanup);
        process.on('uncaughtException', (error) => {
            console.error('❌ Uncaught Exception:', error.message);
            cleanup();
        });
    }

    // Dừng scheduler
    stop() {
        if (this.schedulerInterval) {
            clearInterval(this.schedulerInterval);
            this.schedulerInterval = null;
        }
        this.removeLock();
        this.updateStatus({ isRunning: false });
        console.log('🛑 Scheduler đã dừng');
    }

    // Khởi động lại scheduler
    restart() {
        console.log('🔄 Khởi động lại Scheduler...');
        this.stop();
        this.startScheduler();
    }
}

// Tạo instance singleton
const scraperScheduler = new ScraperScheduler();

module.exports = scraperScheduler;