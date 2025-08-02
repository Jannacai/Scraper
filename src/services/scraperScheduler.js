const cron = require('node-cron');
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const { scrapeXSMB } = require('../../scraper');
const { scrapeXSMT } = require('../../scraperMT');
const { scrapeXSMN } = require('../../scraperMN');
require('dotenv').config();

// Helper function để lấy thời gian Việt Nam
const getVietnamTime = () => {
    const now = new Date();
    return new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Ho_Chi_Minh' }));
};

// Helper function để format ngày
const formatDate = (date) => {
    return date.toLocaleDateString('vi-VN', {
        day: '2-digit',
        month: '2-digit',
        year: 'numeric',
    }).replace(/\//g, '-');
};

// Cấu hình scheduler cho XSMB từ environment variables
const XSMB_CONFIG = {
    schedule: process.env.SCRAPE_SCHEDULE || '14 18 * * *', // Chạy lúc 18h14 mỗi ngày
    station: 'xsmb',
    duration: 20 * 60 * 1000, // 20 phút (từ 18h14 đến 18h34)
    retryAttempts: parseInt(process.env.SCRAPER_RETRY_ATTEMPTS) || 3,
    retryDelay: parseInt(process.env.SCRAPER_RETRY_DELAY) || 5000 // 5 giây
};

// Cấu hình scheduler cho XSMT từ environment variables
const XSMT_CONFIG = {
    schedule: process.env.SCRAPE_SCHEDULE_MT || '14 17 * * *', // Chạy lúc 17h14 mỗi ngày
    station: 'xsmt',
    duration: 20 * 60 * 1000, // 20 phút (từ 17h14 đến 17h34)
    retryAttempts: parseInt(process.env.SCRAPER_RETRY_ATTEMPTS) || 3,
    retryDelay: parseInt(process.env.SCRAPER_RETRY_DELAY) || 5000 // 5 giây
};

// Cấu hình scheduler cho XSMN từ environment variables
const XSMN_CONFIG = {
    schedule: process.env.SCRAPE_SCHEDULE_MN || '24 16 * * *', // Chạy lúc 16h12 mỗi ngày
    station: 'xsmn',
    duration: 30 * 60 * 1000, // 30 phút (từ 16h12 đến 16h42)
    retryAttempts: parseInt(process.env.SCRAPER_RETRY_ATTEMPTS) || 3,
    retryDelay: parseInt(process.env.SCRAPER_RETRY_DELAY) || 5000 // 5 giây
};

// Global state để track scheduler
let schedulerState = {
    xsmb: {
        isRunning: false,
        lastRun: null,
        nextRun: null,
        totalRuns: 0,
        totalErrors: 0
    },
    xsmt: {
        isRunning: false,
        lastRun: null,
        nextRun: null,
        totalRuns: 0,
        totalErrors: 0
    },
    xsmn: {
        isRunning: false,
        lastRun: null,
        nextRun: null,
        totalRuns: 0,
        totalErrors: 0
    }
};

// Worker thread để chạy scraper
const runScraperInWorker = (date, station, maxRetries, scraperType) => {
    return new Promise((resolve, reject) => {
        const path = require('path');
        const workerPath = path.resolve(__dirname, '../workers/scraperWorker.js');

        const worker = new Worker(workerPath);

        worker.on('message', (result) => {
            resolve(result);
        });

        worker.on('error', (error) => {
            reject(error);
        });

        // Gửi data đến worker
        worker.postMessage({
            date,
            station,
            maxRetries,
            scraperType
        });

        // Timeout sau 10 phút
        setTimeout(() => {
            worker.terminate();
            reject(new Error('Worker timeout'));
        }, 10 * 60 * 1000);
    });
};

// Hàm cào dữ liệu với retry logic (sử dụng worker thread)
const scrapeWithRetry = async (date, station, maxRetries = 3, scraperType = 'xsmb') => {
    try {
        console.log(`🔄 Bắt đầu cào dữ liệu ${scraperType.toUpperCase()} cho ngày ${date} (sử dụng worker thread)`);

        const result = await runScraperInWorker(date, station, maxRetries, scraperType);

        if (result.success) {
            console.log(`✅ Cào dữ liệu ${scraperType.toUpperCase()} thành công cho ngày ${date}`);
            schedulerState[scraperType].totalRuns++;
            return true;
        } else {
            console.error(`❌ Cào dữ liệu ${scraperType.toUpperCase()} thất bại cho ngày ${date}`);
            schedulerState[scraperType].totalErrors++;
            return false;
        }
    } catch (error) {
        console.error(`💥 Lỗi worker thread cho ${scraperType.toUpperCase()}:`, error.message);
        schedulerState[scraperType].totalErrors++;
        return false;
    }
};

// Hàm chính để khởi động scheduler cho XSMB
const startScraperScheduler = () => {
    console.log('🚀 Khởi động XSMB Scraper Scheduler (với worker threads)...');
    console.log(`📅 Lịch chạy: ${XSMB_CONFIG.schedule} (18h14 mỗi ngày)`);
    console.log(`⏱️ Thời gian chạy: ${XSMB_CONFIG.duration / 60000} phút`);
    console.log(`🔄 Retry attempts: ${XSMB_CONFIG.retryAttempts}`);
    console.log(`⏳ Retry delay: ${XSMB_CONFIG.retryDelay / 1000} giây`);
    console.log(`🌍 Timezone: ${process.env.TIMEZONE || 'Asia/Ho_Chi_Minh'}`);
    console.log(`⚡ Worker threads: Enabled`);

    // Tính toán thời gian chạy tiếp theo
    const now = getVietnamTime();
    const nextRun = new Date(now);
    nextRun.setHours(18, 14, 0, 0);

    if (now > nextRun) {
        nextRun.setDate(nextRun.getDate() + 1);
    }

    schedulerState.xsmb.nextRun = nextRun;

    // Cron job cho XSMB - chạy lúc 18h14
    const cronJob = cron.schedule(XSMB_CONFIG.schedule, async () => {
        if (schedulerState.xsmb.isRunning) {
            console.log('⚠️ XSMB Scheduler đang chạy, bỏ qua lần này');
            return;
        }

        schedulerState.xsmb.isRunning = true;
        schedulerState.xsmb.lastRun = new Date();

        try {
            const vietnamTime = getVietnamTime();
            const today = formatDate(vietnamTime);

            console.log(`🕐 ${vietnamTime.toLocaleTimeString('vi-VN')} - Bắt đầu cào dữ liệu XSMB cho ngày ${today}`);

            // Bắt đầu cào dữ liệu với worker thread
            const success = await scrapeWithRetry(today, XSMB_CONFIG.station, XSMB_CONFIG.retryAttempts, 'xsmb');

            if (success) {
                console.log(`✅ Hoàn thành cào dữ liệu XSMB cho ngày ${today}`);

                // Tự động dừng sau 20 phút (18h34)
                setTimeout(() => {
                    console.log(`🛑 Dừng cào dữ liệu XSMB lúc ${getVietnamTime().toLocaleTimeString('vi-VN')}`);
                }, XSMB_CONFIG.duration);

            } else {
                console.error(`💥 Thất bại cào dữ liệu XSMB cho ngày ${today} sau ${XSMB_CONFIG.retryAttempts} lần thử`);
            }

        } catch (error) {
            console.error('💥 Lỗi không mong muốn trong XSMB scheduler:', error);
            schedulerState.xsmb.totalErrors++;
        } finally {
            schedulerState.xsmb.isRunning = false;

            // Tính toán thời gian chạy tiếp theo
            const nextRun = new Date();
            nextRun.setDate(nextRun.getDate() + 1);
            nextRun.setHours(18, 14, 0, 0);
            schedulerState.xsmb.nextRun = nextRun;
        }
    }, {
        timezone: process.env.TIMEZONE || 'Asia/Ho_Chi_Minh',
        scheduled: true
    });

    console.log('✅ XSMB Scraper Scheduler đã được khởi động thành công (với worker threads)');

    // Return cron job để có thể dừng sau này
    return cronJob;
};

// Hàm chính để khởi động scheduler cho XSMT
const startXSMTScraperScheduler = () => {
    console.log('🚀 Khởi động XSMT Scraper Scheduler (với worker threads)...');
    console.log(`📅 Lịch chạy: ${XSMT_CONFIG.schedule} (17h14 mỗi ngày)`);
    console.log(`⏱️ Thời gian chạy: ${XSMT_CONFIG.duration / 60000} phút`);
    console.log(`🔄 Retry attempts: ${XSMT_CONFIG.retryAttempts}`);
    console.log(`⏳ Retry delay: ${XSMT_CONFIG.retryDelay / 1000} giây`);
    console.log(`🌍 Timezone: ${process.env.TIMEZONE || 'Asia/Ho_Chi_Minh'}`);
    console.log(`⚡ Worker threads: Enabled`);

    // Tính toán thời gian chạy tiếp theo
    const now = getVietnamTime();
    const nextRun = new Date(now);
    nextRun.setHours(17, 14, 0, 0);

    if (now > nextRun) {
        nextRun.setDate(nextRun.getDate() + 1);
    }

    schedulerState.xsmt.nextRun = nextRun;

    // Cron job cho XSMT - chạy lúc 17h14
    const cronJob = cron.schedule(XSMT_CONFIG.schedule, async () => {
        if (schedulerState.xsmt.isRunning) {
            console.log('⚠️ XSMT Scheduler đang chạy, bỏ qua lần này');
            return;
        }

        schedulerState.xsmt.isRunning = true;
        schedulerState.xsmt.lastRun = new Date();

        try {
            const vietnamTime = getVietnamTime();
            const today = formatDate(vietnamTime);

            console.log(`🕐 ${vietnamTime.toLocaleTimeString('vi-VN')} - Bắt đầu cào dữ liệu XSMT cho ngày ${today}`);

            // Bắt đầu cào dữ liệu với worker thread
            const success = await scrapeWithRetry(today, XSMT_CONFIG.station, XSMT_CONFIG.retryAttempts, 'xsmt');

            if (success) {
                console.log(`✅ Hoàn thành cào dữ liệu XSMT cho ngày ${today}`);

                // Tự động dừng sau 20 phút (17h34)
                setTimeout(() => {
                    console.log(`🛑 Dừng cào dữ liệu XSMT lúc ${getVietnamTime().toLocaleTimeString('vi-VN')}`);
                }, XSMT_CONFIG.duration);

            } else {
                console.error(`💥 Thất bại cào dữ liệu XSMT cho ngày ${today} sau ${XSMT_CONFIG.retryAttempts} lần thử`);
            }

        } catch (error) {
            console.error('💥 Lỗi không mong muốn trong XSMT scheduler:', error);
            schedulerState.xsmt.totalErrors++;
        } finally {
            schedulerState.xsmt.isRunning = false;

            // Tính toán thời gian chạy tiếp theo
            const nextRun = new Date();
            nextRun.setDate(nextRun.getDate() + 1);
            nextRun.setHours(17, 14, 0, 0);
            schedulerState.xsmt.nextRun = nextRun;
        }
    }, {
        timezone: process.env.TIMEZONE || 'Asia/Ho_Chi_Minh',
        scheduled: true
    });

    console.log('✅ XSMT Scraper Scheduler đã được khởi động thành công (với worker threads)');

    // Return cron job để có thể dừng sau này
    return cronJob;
};

// Hàm chính để khởi động scheduler cho XSMN
const startXSMNScraperScheduler = () => {
    console.log('🚀 Khởi động XSMN Scraper Scheduler (với worker threads)...');
    console.log(`📅 Lịch chạy: ${XSMN_CONFIG.schedule} (16h12 mỗi ngày)`);
    console.log(`⏱️ Thời gian chạy: ${XSMN_CONFIG.duration / 60000} phút`);
    console.log(`🔄 Retry attempts: ${XSMN_CONFIG.retryAttempts}`);
    console.log(`⏳ Retry delay: ${XSMN_CONFIG.retryDelay / 1000} giây`);
    console.log(`🌍 Timezone: ${process.env.TIMEZONE || 'Asia/Ho_Chi_Minh'}`);
    console.log(`⚡ Worker threads: Enabled`);

    // Tính toán thời gian chạy tiếp theo
    const now = getVietnamTime();
    const nextRun = new Date(now);
    nextRun.setHours(16, 24, 0, 0);

    if (now > nextRun) {
        nextRun.setDate(nextRun.getDate() + 1);
    }

    schedulerState.xsmn.nextRun = nextRun;

    // Cron job cho XSMN - chạy lúc 16h12
    const cronJob = cron.schedule(XSMN_CONFIG.schedule, async () => {
        if (schedulerState.xsmn.isRunning) {
            console.log('⚠️ XSMN Scheduler đang chạy, bỏ qua lần này');
            return;
        }

        schedulerState.xsmn.isRunning = true;
        schedulerState.xsmn.lastRun = new Date();

        try {
            const vietnamTime = getVietnamTime();
            const today = formatDate(vietnamTime);

            console.log(`🕐 ${vietnamTime.toLocaleTimeString('vi-VN')} - Bắt đầu cào dữ liệu XSMN cho ngày ${today}`);

            // Bắt đầu cào dữ liệu với worker thread
            const success = await scrapeWithRetry(today, XSMN_CONFIG.station, XSMN_CONFIG.retryAttempts, 'xsmn');

            if (success) {
                console.log(`✅ Hoàn thành cào dữ liệu XSMN cho ngày ${today}`);

                // Tự động dừng sau 30 phút (16h42)
                setTimeout(() => {
                    console.log(`🛑 Dừng cào dữ liệu XSMN lúc ${getVietnamTime().toLocaleTimeString('vi-VN')}`);
                }, XSMN_CONFIG.duration);

            } else {
                console.error(`💥 Thất bại cào dữ liệu XSMN cho ngày ${today} sau ${XSMN_CONFIG.retryAttempts} lần thử`);
            }

        } catch (error) {
            console.error('💥 Lỗi không mong muốn trong XSMN scheduler:', error);
            schedulerState.xsmn.totalErrors++;
        } finally {
            schedulerState.xsmn.isRunning = false;

            // Tính toán thời gian chạy tiếp theo
            const nextRun = new Date();
            nextRun.setDate(nextRun.getDate() + 1);
            nextRun.setHours(16, 24, 0, 0);
            schedulerState.xsmn.nextRun = nextRun;
        }
    }, {
        timezone: process.env.TIMEZONE || 'Asia/Ho_Chi_Minh',
        scheduled: true
    });

    console.log('✅ XSMN Scraper Scheduler đã được khởi động thành công (với worker threads)');

    // Return cron job để có thể dừng sau này
    return cronJob;
};

// Hàm để dừng scheduler
const stopScraperScheduler = () => {
    console.log('🛑 Dừng XSMB, XSMT & XSMN Scraper Scheduler...');
    schedulerState.xsmb.isRunning = false;
    schedulerState.xsmt.isRunning = false;
    schedulerState.xsmn.isRunning = false;
    // Cron sẽ tự động dừng khi process kết thúc
};

// Hàm để test scheduler ngay lập tức
const testScraper = async (scraperType = 'xsmb') => {
    const vietnamTime = getVietnamTime();
    const today = formatDate(vietnamTime);

    console.log(`🧪 Test cào dữ liệu ${scraperType.toUpperCase()} cho ngày ${today} (với worker thread)`);
    const success = await scrapeWithRetry(today, scraperType === 'xsmt' ? XSMT_CONFIG.station : scraperType === 'xsmn' ? XSMN_CONFIG.station : XSMB_CONFIG.station, 1, scraperType);

    if (success) {
        console.log(`✅ Test cào dữ liệu ${scraperType.toUpperCase()} thành công`);
    } else {
        console.error(`❌ Test cào dữ liệu ${scraperType.toUpperCase()} thất bại`);
    }

    return success;
};

// Hàm để lấy trạng thái scheduler
const getSchedulerStatus = () => {
    return {
        xsmb: {
            ...schedulerState.xsmb,
            config: XSMB_CONFIG,
            isActive: true,
            timezone: process.env.TIMEZONE || 'Asia/Ho_Chi_Minh'
        },
        xsmt: {
            ...schedulerState.xsmt,
            config: XSMT_CONFIG,
            isActive: true,
            timezone: process.env.TIMEZONE || 'Asia/Ho_Chi_Minh'
        },
        xsmn: {
            ...schedulerState.xsmn,
            config: XSMN_CONFIG,
            isActive: true,
            timezone: process.env.TIMEZONE || 'Asia/Ho_Chi_Minh'
        },
        environment: {
            nodeEnv: process.env.NODE_ENV,
            timezone: process.env.TIMEZONE,
            schedule: process.env.SCRAPE_SCHEDULE,
            scheduleMT: process.env.SCRAPE_SCHEDULE_MT,
            scheduleMN: process.env.SCRAPE_SCHEDULE_MN
        },
        performance: {
            workerThreads: true,
            memoryUsage: process.memoryUsage(),
            uptime: process.uptime()
        }
    };
};

module.exports = {
    startScraperScheduler,
    startXSMTScraperScheduler,
    startXSMNScraperScheduler,
    stopScraperScheduler,
    testScraper,
    getSchedulerStatus,
    XSMB_CONFIG,
    XSMT_CONFIG,
    XSMN_CONFIG
};