const { scrapeXSMB } = require('../../scraper');
const { connectMongoDB } = require('../../db');

class ScraperScheduler {
    constructor() {
        this.scheduler = null;
        this.isRunning = false;
        this.lastRunDate = null;
        this.timezone = 'Asia/Ho_Chi_Minh';

        // ✅ TỐI ƯU: Cache thời gian để tránh tính toán lại
        this.cachedVietnamTime = null;
        this.lastCacheTime = 0;
        this.cacheDuration = 1000; // Cache 1 giây

        // ✅ TỐI ƯU: Cache trạng thái để tránh tính toán lại
        this.cachedStatus = null;
        this.lastStatusCache = 0;
        this.statusCacheDuration = 5000; // Cache 5 giây

        // ✅ TỐI ƯU: Tính toán thời gian kích hoạt tiếp theo
        this.nextTriggerTime = null;
        this.updateNextTriggerTime();
    }

    // ✅ TỐI ƯU: Cache thời gian Việt Nam
    getVietnamTime() {
        const now = Date.now();
        if (!this.cachedVietnamTime || (now - this.lastCacheTime) > this.cacheDuration) {
            this.cachedVietnamTime = new Date(new Date().toLocaleString('en-US', { timeZone: this.timezone }));
            this.lastCacheTime = now;
        }
        return this.cachedVietnamTime;
    }

    // Format ngày theo định dạng DD/MM/YYYY
    formatDate(date) {
        const day = String(date.getDate()).padStart(2, '0');
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const year = date.getFullYear();
        return `${day}/${month}/${year}`;
    }

    // ✅ TỐI ƯU: Cập nhật thời gian kích hoạt tiếp theo
    updateNextTriggerTime() {
        const now = this.getVietnamTime();
        this.nextTriggerTime = new Date(now);
        this.nextTriggerTime.setHours(18, 14, 0, 0);

        // Nếu đã qua 18h14 hôm nay, tính cho ngày mai
        if (now.getHours() > 18 || (now.getHours() === 18 && now.getMinutes() >= 14)) {
            this.nextTriggerTime.setDate(this.nextTriggerTime.getDate() + 1);
        }
    }

    // ✅ TỐI ƯU: Kiểm tra thời gian kích hoạt hiệu quả hơn
    shouldTriggerScraper() {
        const now = this.getVietnamTime();
        const nowTime = now.getTime();

        // Kiểm tra xem đã đến thời gian kích hoạt chưa
        if (nowTime >= this.nextTriggerTime.getTime()) {
            // Cập nhật thời gian kích hoạt tiếp theo
            this.updateNextTriggerTime();
            return true;
        }

        return false;
    }

    // ✅ TỐI ƯU: Kiểm tra đã chạy hôm nay - đơn giản hóa
    hasRunToday() {
        const today = this.formatDate(this.getVietnamTime());
        return this.lastRunDate === today;
    }

    // Kích hoạt scraper
    async triggerScraper() {
        try {
            const vietnamTime = this.getVietnamTime();
            const today = this.formatDate(vietnamTime);

            // Kiểm tra xem đã chạy hôm nay chưa
            if (this.hasRunToday()) {
                console.log(`🔄 Scraper đã chạy hôm nay (${today}), bỏ qua`);
                return;
            }

            console.log(`🚀 Tự động kích hoạt scraper cho ngày ${today} - XSMB`);

            // Đảm bảo kết nối MongoDB
            await connectMongoDB();

            // Kích hoạt scraper
            await scrapeXSMB(today, 'xsmb', false);

            // Cập nhật ngày chạy cuối
            this.lastRunDate = today;

            console.log(`✅ Đã kích hoạt scraper thành công cho ngày ${today}`);

        } catch (error) {
            console.error('❌ Lỗi khi kích hoạt scraper tự động:', error.message);
        }
    }

    // ✅ TỐI ƯU: Khởi động scheduler với interval thông minh
    start() {
        if (this.isRunning) {
            console.log('⚠️ Scheduler đã đang chạy');
            return;
        }

        console.log('🕐 Khởi động Scraper Scheduler...');
        console.log(`⏰ Sẽ kích hoạt scraper vào 18h14 hàng ngày (múi giờ: ${this.timezone})`);

        this.isRunning = true;

        // Chạy ngay lập tức nếu đang trong khung giờ kích hoạt
        if (this.shouldTriggerScraper()) {
            console.log('⚡ Đang trong khung giờ kích hoạt, chạy ngay lập tức');
            this.triggerScraper();
        }

        // ✅ TỐI ƯU: Sử dụng interval thông minh thay vì check mỗi giây
        const checkInterval = () => {
            if (this.shouldTriggerScraper()) {
                this.triggerScraper();
            }
        };

        // Check ngay lập tức
        checkInterval();

        // ✅ TỐI ƯU: Tính toán interval thông minh
        const calculateInterval = () => {
            const now = this.getVietnamTime();
            const timeToNext = this.nextTriggerTime.getTime() - now.getTime();

            // Nếu còn ít hơn 1 phút, check mỗi 10 giây
            if (timeToNext < 60000) {
                return 10000; // 10 giây
            }
            // Nếu còn ít hơn 1 giờ, check mỗi phút
            else if (timeToNext < 3600000) {
                return 60000; // 1 phút
            }
            // Nếu còn nhiều thời gian, check mỗi 5 phút
            else {
                return 300000; // 5 phút
            }
        };

        // Thiết lập interval động
        const setDynamicInterval = () => {
            if (this.scheduler) {
                clearInterval(this.scheduler);
            }

            const interval = calculateInterval();
            this.scheduler = setInterval(() => {
                checkInterval();
                // Cập nhật interval nếu cần
                const newInterval = calculateInterval();
                if (newInterval !== interval) {
                    setDynamicInterval();
                }
            }, interval);
        };

        setDynamicInterval();

        console.log('✅ Scraper Scheduler đã khởi động thành công');
    }

    // Dừng scheduler
    stop() {
        if (this.scheduler) {
            clearInterval(this.scheduler);
            this.scheduler = null;
        }
        this.isRunning = false;
        console.log('🛑 Đã dừng Scraper Scheduler');
    }

    // ✅ TỐI ƯU: Cache trạng thái để tránh tính toán lại
    getStatus() {
        const now = Date.now();
        if (this.cachedStatus && (now - this.lastStatusCache) < this.statusCacheDuration) {
            return this.cachedStatus;
        }

        const vietnamTime = this.getVietnamTime();
        const timeUntilNextRun = this.nextTriggerTime.getTime() - vietnamTime.getTime();
        const hours = Math.floor(timeUntilNextRun / (1000 * 60 * 60));
        const minutes = Math.floor((timeUntilNextRun % (1000 * 60 * 60)) / (1000 * 60));
        const seconds = Math.floor((timeUntilNextRun % (1000 * 60)) / 1000);

        this.cachedStatus = {
            isRunning: this.isRunning,
            lastRunDate: this.lastRunDate,
            nextRun: this.nextTriggerTime.toLocaleString('vi-VN', { timeZone: this.timezone }),
            timeUntilNextRun: `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}`,
            currentTime: vietnamTime.toLocaleString('vi-VN', { timeZone: this.timezone })
        };

        this.lastStatusCache = now;
        return this.cachedStatus;
    }

    // Kích hoạt thủ công (cho testing)
    async manualTrigger(date = null, station = 'xsmb') {
        try {
            const targetDate = date || this.formatDate(this.getVietnamTime());
            console.log(`🔧 Kích hoạt thủ công scraper cho ngày ${targetDate} - ${station}`);

            await connectMongoDB();
            await scrapeXSMB(targetDate, station, false);

            console.log(`✅ Kích hoạt thủ công thành công cho ngày ${targetDate}`);
        } catch (error) {
            console.error('❌ Lỗi khi kích hoạt thủ công:', error.message);
            throw error;
        }
    }
}

// Tạo instance singleton
const scraperScheduler = new ScraperScheduler();

module.exports = scraperScheduler;