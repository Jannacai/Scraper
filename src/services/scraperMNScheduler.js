const { scrapeXSMN } = require('../../scraperMN');
const { connectMongoDB } = require('../../db');

class ScraperMNScheduler {
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

        // ✅ TỐI ƯU: Cache provinces theo ngày trong tuần
        this.provincesByDay = {
            1: [ // Thứ 2
                { tinh: 'tphcm', tentinh: 'TP.HCM' },
                { tinh: 'dong-thap', tentinh: 'Đồng Tháp' },
                { tinh: 'ca-mau', tentinh: 'Cà Mau' },
            ],
            2: [ // Thứ 3
                { tinh: 'ben-tre', tentinh: 'Bến Tre' },
                { tinh: 'vung-tau', tentinh: 'Vũng Tàu' },
                { tinh: 'bac-lieu', tentinh: 'Bạc Liêu' },
            ],
            3: [ // Thứ 4
                { tinh: 'dong-nai', tentinh: 'Đồng Nai' },
                { tinh: 'can-tho', tentinh: 'Cần Thơ' },
                { tinh: 'soc-trang', tentinh: 'Sóc Trăng' },
            ],
            4: [ // Thứ 5
                { tinh: 'tay-ninh', tentinh: 'Tây Ninh' },
                { tinh: 'an-giang', tentinh: 'An Giang' },
                { tinh: 'binh-thuan', tentinh: 'Bình Thuận' },
            ],
            5: [ // Thứ 6
                { tinh: 'vinh-long', tentinh: 'Vĩnh Long' },
                { tinh: 'binh-duong', tentinh: 'Bình Dương' },
                { tinh: 'tra-vinh', tentinh: 'Trà Vinh' },
            ],
            6: [ // Thứ 7
                { tinh: 'tphcm', tentinh: 'TP.HCM' },
                { tinh: 'long-an', tentinh: 'Long An' },
                { tinh: 'binh-phuoc', tentinh: 'Bình Phước' },
                { tinh: 'hau-giang', tentinh: 'Hậu Giang' },
            ],
            0: [ // Chủ nhật
                { tinh: 'tien-giang', tentinh: 'Tiền Giang' },
                { tinh: 'kien-giang', tentinh: 'Kiên Giang' },
                { tinh: 'da-lat', tentinh: 'Đà Lạt' },
            ],
        };
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

    // ✅ TỐI ƯU: Lấy provinces theo ngày trong tuần
    getProvincesForToday() {
        const vietnamTime = this.getVietnamTime();
        const dayOfWeek = vietnamTime.getDay(); // 0 = Chủ nhật, 1 = Thứ 2, ...
        return this.provincesByDay[dayOfWeek] || [];
    }

    // ✅ TỐI ƯU: Cập nhật thời gian kích hoạt tiếp theo
    updateNextTriggerTime() {
        const now = this.getVietnamTime();
        this.nextTriggerTime = new Date(now);
        this.nextTriggerTime.setHours(16, 13, 0, 0); // 16h12 cho XSMN

        // Nếu đã qua 16h12 hôm nay, tính cho ngày mai
        if (now.getHours() > 16 || (now.getHours() === 16 && now.getMinutes() >= 13)) {
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
            const provinces = this.getProvincesForToday();

            // Kiểm tra xem đã chạy hôm nay chưa
            if (this.hasRunToday()) {
                console.log(`🔄 Scraper XSMN đã chạy hôm nay (${today}), bỏ qua`);
                return;
            }

            // Kiểm tra có provinces cho ngày hôm nay không
            if (provinces.length === 0) {
                console.log(`⚠️ Không có provinces cho ngày ${today} (${vietnamTime.toLocaleDateString('vi-VN', { weekday: 'long' })}), bỏ qua`);
                return;
            }

            console.log(`🚀 Tự động kích hoạt scraper XSMN cho ngày ${today} - ${provinces.length} tỉnh`);
            console.log(`📋 Provinces: ${provinces.map(p => p.tentinh).join(', ')}`);

            // Đảm bảo kết nối MongoDB
            await connectMongoDB();

            // Kích hoạt scraper cho tất cả provinces cùng lúc
            try {
                console.log(`🔄 Đang cào dữ liệu cho ${provinces.length} tỉnh: ${provinces.map(p => p.tentinh).join(', ')}`);
                await scrapeXSMN(today, 'xsmn', provinces);
                console.log(`✅ Đã cào xong tất cả ${provinces.length} tỉnh`);
            } catch (error) {
                console.error(`❌ Lỗi khi cào dữ liệu XSMN:`, error.message);
            }

            // Cập nhật ngày chạy cuối
            this.lastRunDate = today;

            console.log(`✅ Đã kích hoạt scraper XSMN thành công cho ngày ${today}`);

        } catch (error) {
            console.error('❌ Lỗi khi kích hoạt scraper XSMN tự động:', error.message);
        }
    }

    // ✅ TỐI ƯU: Khởi động scheduler với interval thông minh
    start() {
        if (this.isRunning) {
            console.log('⚠️ XSMN Scheduler đã đang chạy');
            return;
        }

        console.log('🕐 Khởi động XSMN Scraper Scheduler...');
        console.log(`⏰ Sẽ kích hoạt scraper vào 16h12 hàng ngày (múi giờ: ${this.timezone})`);

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

        console.log('✅ XSMN Scraper Scheduler đã khởi động thành công');
    }

    // Dừng scheduler
    stop() {
        if (this.scheduler) {
            clearInterval(this.scheduler);
            this.scheduler = null;
        }
        this.isRunning = false;
        console.log('🛑 Đã dừng XSMN Scraper Scheduler');
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

        const provinces = this.getProvincesForToday();

        this.cachedStatus = {
            isRunning: this.isRunning,
            lastRunDate: this.lastRunDate,
            nextRun: this.nextTriggerTime.toLocaleString('vi-VN', { timeZone: this.timezone }),
            timeUntilNextRun: `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}`,
            currentTime: vietnamTime.toLocaleString('vi-VN', { timeZone: this.timezone }),
            todayProvinces: provinces.map(p => p.tentinh),
            todayDayOfWeek: vietnamTime.toLocaleDateString('vi-VN', { weekday: 'long' })
        };

        this.lastStatusCache = now;
        return this.cachedStatus;
    }

    // Kích hoạt thủ công (cho testing)
    async manualTrigger(date = null, station = 'xsmn', provinces = null) {
        try {
            const targetDate = date || this.formatDate(this.getVietnamTime());
            const targetProvinces = provinces || this.getProvincesForToday();

            console.log(`🔧 Kích hoạt thủ công scraper XSMN cho ngày ${targetDate} - ${station} - ${targetProvinces.length} tỉnh`);

            await connectMongoDB();
            await scrapeXSMN(targetDate, station, targetProvinces);

            console.log(`✅ Kích hoạt thủ công thành công cho ngày ${targetDate}`);
        } catch (error) {
            console.error('❌ Lỗi khi kích hoạt thủ công XSMN:', error.message);
            throw error;
        }
    }
}

// Tạo instance singleton
const scraperMNScheduler = new ScraperMNScheduler();

module.exports = scraperMNScheduler; 