const { parentPort } = require('worker_threads');
const path = require('path');

// Import scraper functions
let scrapeXSMB, scrapeXSMT, scrapeXSMN;

try {
    const scraperPath = path.resolve(__dirname, '../../scraper.js');
    const scraperMTPath = path.resolve(__dirname, '../../scraperMT.js');
    const scraperMNPath = path.resolve(__dirname, '../../scraperMN.js');

    const scraperModule = require(scraperPath);
    const scraperMTModule = require(scraperMTPath);
    const scraperMNModule = require(scraperMNPath);

    scrapeXSMB = scraperModule.scrapeXSMB;
    scrapeXSMT = scraperMTModule.scrapeXSMT;
    scrapeXSMN = scraperMNModule.scrapeXSMN;
} catch (error) {
    console.error('❌ Lỗi import scraper modules:', error.message);
    parentPort.postMessage({ success: false, error: error.message });
    return;
}

// Hàm cào dữ liệu với retry logic
async function scrapeWithRetry(date, station, maxRetries, scraperType) {
    const scraperFunction = scraperType === 'xsmt' ? scrapeXSMT : scraperType === 'xsmn' ? scrapeXSMN : scrapeXSMB;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            console.log(`🔄 Bắt đầu cào dữ liệu ${scraperType.toUpperCase()} lần thử ${attempt}/${maxRetries} cho ngày ${date}`);
            await scraperFunction(date, station);
            console.log(`✅ Cào dữ liệu ${scraperType.toUpperCase()} thành công cho ngày ${date}`);
            return true;
        } catch (error) {
            console.error(`❌ Lỗi cào dữ liệu ${scraperType.toUpperCase()} lần thử ${attempt}/${maxRetries}:`, error.message);
            if (attempt === maxRetries) {
                console.error(`💥 Đã thử ${maxRetries} lần nhưng vẫn thất bại cho ngày ${date}`);
                return false;
            }
            await new Promise(resolve => setTimeout(resolve, 5000));
        }
    }
}

// Nhận message từ main thread
parentPort.on('message', async (data) => {
    const { date, station, maxRetries, scraperType } = data;

    try {
        const result = await scrapeWithRetry(date, station, maxRetries, scraperType);
        parentPort.postMessage({ success: result });
    } catch (error) {
        parentPort.postMessage({ success: false, error: error.message });
    }
}); 