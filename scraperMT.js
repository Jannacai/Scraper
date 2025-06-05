const puppeteer = require('puppeteer');
const cheerio = require('cheerio');
const mongoose = require('mongoose');
const schedule = require('node-schedule');
const redis = require('redis');
const { lock } = require('proper-lockfile');
const fs = require('fs');
const path = require('path');
const pidusage = require('pidusage');
require('dotenv').config();

const XSMT = require('./src/models/XS_MT.models');

// Kết nối MongoDB
mongoose.connect(process.env.MONGODB_URI || 'mongodb://localhost:27017/xsmt', {
    maxPoolSize: 20,
    minPoolSize: 2,
}).then(() => console.log('Đã kết nối MongoDB')).catch(err => console.error('Lỗi kết nối MongoDB:', err));

// Kết nối Redis
const redisClient = redis.createClient({
    url: process.env.REDIS_URL || 'redis://localhost:6379',
});
redisClient.connect().catch(err => console.error('Lỗi kết nối Redis:', err));

// File lock
const lockFilePath = path.resolve(__dirname, 'xsmt_scraper.lock');
const ensureLockFile = () => {
    try {
        if (!fs.existsSync(lockFilePath)) {
            fs.writeFileSync(lockFilePath, '');
            console.log(`Tạo file ${lockFilePath}`);
        }
    } catch (error) {
        console.error(`Lỗi khi tạo file ${lockFilePath}:`, error.message);
        throw error;
    }
};

// Hàm chuyển đổi tên tỉnh sang dạng kebab-case
function toKebabCase(str) {
    return str
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .replace(/đ/g, 'd')
        .replace(/Đ/g, 'D')
        .toLowerCase()
        .replace(/\s+/g, '-')
        .replace(/[^a-z0-9-]/g, '');
}

// Hàm định dạng ngày thành DD-MM-YYYY
function formatDateToDDMMYYYY(date) {
    const day = String(date.getDate()).padStart(2, '0');
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const year = date.getFullYear();
    return `${day}-${month}-${year}`;
}

// Hàm kiểm tra dữ liệu đầy đủ cho một tỉnh
function isDataComplete(result) {
    return (
        result.tentinh && result.tentinh.length >= 1 && !result.tentinh.startsWith('Tỉnh_') &&
        result.specialPrize && result.specialPrize.length >= 1 && !result.specialPrize.includes('...') &&
        result.firstPrize && result.firstPrize.length >= 1 && !result.firstPrize.includes('...') &&
        result.secondPrize && result.secondPrize.length >= 1 && !result.secondPrize.includes('...') &&
        result.threePrizes && result.threePrizes.length >= 2 && !result.threePrizes.includes('...') &&
        result.fourPrizes && result.fourPrizes.length >= 7 && !result.fourPrizes.includes('...') &&
        result.fivePrizes && result.fivePrizes.length >= 1 && !result.fivePrizes.includes('...') &&
        result.sixPrizes && result.sixPrizes.length >= 3 && !result.sixPrizes.includes('...') &&
        result.sevenPrizes && result.sevenPrizes.length >= 1 && !result.sevenPrizes.includes('...') &&
        result.eightPrizes && result.eightPrizes.length >= 1 && !result.eightPrizes.includes('...')
    );
}

// Hàm kiểm tra xem có dữ liệu nào đáng lưu không
function hasAnyData(result) {
    return (
        result.tentinh && result.tentinh.length >= 1 && !result.tentinh.startsWith('Tỉnh_') &&
        (
            (result.specialPrize && result.specialPrize.length >= 1 && !result.specialPrize.includes('...')) ||
            (result.firstPrize && result.firstPrize.length >= 1 && !result.firstPrize.includes('...')) ||
            (result.secondPrize && result.secondPrize.length >= 1 && !result.secondPrize.includes('...')) ||
            (result.threePrizes && result.threePrizes.length >= 1 && !result.threePrizes.includes('...')) ||
            (result.fourPrizes && result.fourPrizes.length >= 1 && !result.fourPrizes.includes('...')) ||
            (result.fivePrizes && result.fivePrizes.length >= 1 && !result.fivePrizes.includes('...')) ||
            (result.sixPrizes && result.sixPrizes.length >= 1 && !result.sixPrizes.includes('...')) ||
            (result.sevenPrizes && result.sevenPrizes.length >= 1 && !result.sevenPrizes.includes('...')) ||
            (result.eightPrizes && result.eightPrizes.length >= 1 && !result.eightPrizes.includes('...'))
        )
    );
}

// Hàm publish dữ liệu lên Redis
async function publishToRedis(prizeType, prizeData, additionalData) {
    const { drawDate, tentinh, tinh, year, month } = additionalData;
    const today = formatDateToDDMMYYYY(new Date(drawDate));
    const message = JSON.stringify({
        prizeType,
        prizeData,
        drawDate: today,
        tentinh,
        tinh,
        year,
        month,
    });
    console.log(`Chuẩn bị gửi Redis: ${prizeType} cho tỉnh ${tentinh}`, prizeData, `Kênh: xsmt:${today}:${tinh}`);

    try {
        if (!redisClient.isOpen) {
            console.log('Redis client chưa sẵn sàng, kết nối lại...');
            await redisClient.connect();
            console.log('Kết nối Redis thành công');
        }
        await Promise.all([
            redisClient.publish(`xsmt:${today}:${tinh}`, message),
            redisClient.hSet(`kqxs:xsmt:${today}:${tinh}`, prizeType, JSON.stringify(prizeData)),
            redisClient.hSet(`kqxs:xsmt:${today}:${tinh}:meta`, 'metadata', JSON.stringify({ tentinh, tinh, year, month })),
            redisClient.expire(`kqxs:xsmt:${today}:${tinh}`, 7200),
            redisClient.expire(`kqxs:xsmt:${today}:${tinh}:meta`, 7200),
        ]);
        console.log(`Đã gửi ${prizeType} và metadata qua Redis cho ngày ${today}, tỉnh ${tentinh}`);
    } catch (error) {
        console.error(`Lỗi gửi Redis (${prizeType}, tỉnh ${tentinh}):`, error.message);
        throw error;
    }
}

// Hàm lưu dữ liệu vào MongoDB
async function saveToMongoDB(result) {
    try {
        const dateObj = new Date(result.drawDate);
        const existingResult = await XSMT.findOne({ drawDate: dateObj, station: result.station, tentinh: result.tentinh }).lean();

        if (existingResult) {
            const existingData = {
                specialPrize: existingResult.specialPrize,
                firstPrize: existingResult.firstPrize,
                secondPrize: existingResult.secondPrize,
                threePrizes: existingResult.threePrizes,
                fourPrizes: existingResult.fourPrizes,
                fivePrizes: existingResult.fivePrizes,
                sixPrizes: existingResult.sixPrizes,
                sevenPrizes: existingResult.sevenPrizes,
                eightPrizes: existingResult.eightPrizes,
            };

            const newData = {
                specialPrize: result.specialPrize,
                firstPrize: result.firstPrize,
                secondPrize: result.secondPrize,
                threePrizes: result.threePrizes,
                fourPrizes: result.fourPrizes,
                fivePrizes: result.fivePrizes,
                sixPrizes: result.sixPrizes,
                sevenPrizes: result.sevenPrizes,
                eightPrizes: result.eightPrizes,
            };

            if (JSON.stringify(existingData) !== JSON.stringify(newData)) {
                await XSMT.updateOne(
                    { drawDate: dateObj, station: result.station, tentinh: result.tentinh },
                    { $set: result },
                    { upsert: true }
                );
                console.log(`Cập nhật kết quả ngày ${result.drawDate.toISOString().split('T')[0]} cho tỉnh ${result.tentinh}`);
            } else {
                console.log(`Dữ liệu ngày ${result.drawDate.toISOString().split('T')[0]} cho tỉnh ${result.tentinh} không thay đổi`);
            }
        } else {
            await XSMT.create(result);
            console.log(`Lưu kết quả mới ngày ${result.drawDate.toISOString().split('T')[0]} cho tỉnh ${result.tentinh}`);
        }
    } catch (error) {
        console.error(`Lỗi khi lưu dữ liệu ngày ${result.drawDate.toISOString().split('T')[0]} cho tỉnh ${result.tentinh}:`, error.message);
    }
}

// Hàm ghi log hiệu suất
async function logPerformance(startTime, iteration, success, province) {
    const stats = await pidusage(process.pid);
    const duration = (Date.now() - startTime) / 1000;
    console.log(`Lần cào ${iteration} cho tỉnh ${province} (${success ? 'Thành công' : 'Thất bại'}):`, {
        duration: `${duration.toFixed(2)}s`,
        cpu: `${stats.cpu.toFixed(2)}%`,
        memory: `${(stats.memory / 1024 / 1024).toFixed(2)}MB`,
    });
}

// Hàm cào dữ liệu XSMT
async function scrapeXSMT(date, station) {
    let browser;
    let page;
    let intervalId;
    let release;
    let iteration = 0;
    let successCount = 0;
    let errorCount = 0;
    const startTime = Date.now();
    let lastPrizeDataByProvince = {};

    try {
        const dateParts = date.split('/');
        const dateObj = new Date(`${dateParts[2]}-${dateParts[1]}-${dateParts[0]}`);
        const formattedDate = date.replace(/\//g, '-');

        ensureLockFile();
        release = await lock(lockFilePath, { retries: 3, stale: 10000 });

        browser = await puppeteer.launch({
            headless: 'new',
            args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
            executablePath: process.env.CHROMIUM_PATH || undefined,
        });
        page = await browser.newPage();
        await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124');
        await page.setRequestInterception(true);
        page.on('request', (req) => {
            if (['image', 'stylesheet', 'font', 'script', 'media'].includes(req.resourceType())) {
                req.abort();
            } else {
                req.continue();
            }
        });

        let baseUrl;
        if (station.toLowerCase() === 'xsmt') {
            baseUrl = `https://xosovn.com/xsmt-${formattedDate}`;
            console.log(`Đang cào dữ liệu từ: ${baseUrl}`);
        } else {
            throw new Error('Chỉ hỗ trợ đài xsmt trong phiên bản này');
        }

        const scrapeAndSave = async () => {
            iteration += 1;
            const iterationStart = Date.now();
            try {
                const now = new Date();
                const isLiveWindow = now.getHours() === 16 && now.getMinutes() >= 15 && now.getMinutes() <= 35;
                const intervalMs = isLiveWindow ? 3000 : 30000;

                let attempt = 0;
                const maxAttempts = 3;
                while (attempt < maxAttempts) {
                    try {
                        await page.goto(baseUrl, { waitUntil: 'domcontentloaded', timeout: 15000 });
                        await page.waitForSelector('span[class*="v-g4-"]', { timeout: 5000 }).catch(() => {
                            console.log('Chưa thấy giải 4, tiếp tục cào...');
                        });
                        break;
                    } catch (error) {
                        attempt++;
                        console.warn(`Lỗi request lần ${attempt}: ${error.message}. Thử lại...`);
                        if (attempt === maxAttempts) throw error;
                        await new Promise(resolve => setTimeout(resolve, 1000));
                    }
                }

                const html = await page.content();
                const $ = cheerio.load(html);
                const resultTable = $('table.kqsx-mt');

                if (resultTable.length === 0) {
                    console.log('Không tìm thấy bảng kết quả với class="kqsx-mt". Có thể trang không có dữ liệu hoặc cấu trúc HTML đã thay đổi.');
                    await logPerformance(iterationStart, iteration, false, 'N/A');
                    errorCount += 1;
                    return;
                }

                const provinceRow = resultTable.find('tr.bg-pr');
                const provinces = [];
                provinceRow.find('th').each((i, elem) => {
                    if (i === 0) return;
                    const provinceName = $(elem).find('a').text().trim();
                    if (provinceName) {
                        provinces.push(provinceName);
                    }
                });

                if (provinces.length === 0) {
                    console.log('Không tìm thấy tỉnh nào trong bảng kết quả.');
                    await logPerformance(iterationStart, iteration, false, 'N/A');
                    errorCount += 1;
                    return;
                }

                provinces.forEach(province => {
                    if (!lastPrizeDataByProvince[province]) {
                        lastPrizeDataByProvince[province] = {
                            specialPrize: ['...'],
                            firstPrize: ['...'],
                            secondPrize: ['...'],
                            threePrizes: ['...', '...'],
                            fourPrizes: ['...', '...', '...', '...', '...', '...', '...'],
                            fivePrizes: ['...'],
                            sixPrizes: ['...', '...', '...'],
                            sevenPrizes: ['...'],
                            eightPrizes: ['...'],
                        };
                    }
                });

                const provincesData = {};
                provinces.forEach(province => {
                    provincesData[province] = {
                        specialPrize: [],
                        firstPrize: [],
                        secondPrize: [],
                        threePrizes: [],
                        fourPrizes: [],
                        fivePrizes: [],
                        sixPrizes: [],
                        sevenPrizes: [],
                        eightPrizes: [],
                    };
                });

                resultTable.find('tr').each((i, row) => {
                    const rowClass = $(row).attr('class') || 'undefined';
                    if (rowClass === 'bg-pr') return;

                    $(row).find('td').each((j, cell) => {
                        if (j === 0) return;
                        const province = provinces[j - 1];
                        if (!province) return;

                        const spans = $(cell).find('span');
                        if (spans.length === 0) return;

                        spans.each((k, span) => {
                            const spanClass = $(span).attr('class')?.trim();
                            const result = $(span).text().trim();
                            if (!spanClass || !result) return;

                            let prizeType;
                            let maxResults;

                            if (spanClass.includes('v-gdb')) {
                                prizeType = 'specialPrize';
                                maxResults = 1;
                            } else if (spanClass.includes('v-g1')) {
                                prizeType = 'firstPrize';
                                maxResults = 1;
                            } else if (spanClass.includes('v-g2')) {
                                prizeType = 'secondPrize';
                                maxResults = 1;
                            } else if (spanClass.includes('v-g3')) {
                                prizeType = 'threePrizes';
                                maxResults = 2;
                            } else if (spanClass.includes('v-g4')) {
                                prizeType = 'fourPrizes';
                                maxResults = 7;
                            } else if (spanClass.includes('v-g5')) {
                                prizeType = 'fivePrizes';
                                maxResults = 1;
                            } else if (spanClass.includes('v-g6')) {
                                prizeType = 'sixPrizes';
                                maxResults = 3;
                            } else if (spanClass.includes('v-g7')) {
                                prizeType = 'sevenPrizes';
                                maxResults = 1;
                            } else if (spanClass.includes('v-g8')) {
                                prizeType = 'eightPrizes';
                                maxResults = 1;
                            } else {
                                return;
                            }

                            const currentResults = provincesData[province][prizeType];
                            const remainingSlots = maxResults - currentResults.length;
                            if (remainingSlots <= 0) return;

                            currentResults.push(result);
                        });
                    });
                });

                const drawDate = $('.ngay_quay, .draw-date, .date, h1.df-title').first().text().trim().match(/\d{2}\/\d{2}\/\d{4}/)?.[0] || date;
                const dateObj = new Date(drawDate.split('/').reverse().join('-'));
                const daysOfWeek = ['Chủ nhật', 'Thứ 2', 'Thứ 3', 'Thứ 4', 'Thứ 5', 'Thứ 6', 'Thứ 7'];
                const dayOfWeek = daysOfWeek[dateObj.getDay()];

                let allProvincesComplete = true;

                for (const tentinh of provinces) {
                    const tinh = toKebabCase(tentinh);
                    const slug = `xsmt-${formattedDate}-${tinh}`;

                    const formattedResult = {
                        drawDate: dateObj,
                        slug,
                        year: dateObj.getFullYear(),
                        month: dateObj.getMonth() + 1,
                        dayOfWeek,
                        tentinh,
                        tinh,
                        specialPrize: provincesData[tentinh]?.specialPrize.length ? provincesData[tentinh].specialPrize : ['...'],
                        firstPrize: provincesData[tentinh]?.firstPrize.length ? provincesData[tentinh].firstPrize : ['...'],
                        secondPrize: provincesData[tentinh]?.secondPrize.length ? provincesData[tentinh].secondPrize : ['...'],
                        threePrizes: provincesData[tentinh]?.threePrizes.length ? provincesData[tentinh].threePrizes : ['...', '...'],
                        fourPrizes: provincesData[tentinh]?.fourPrizes.length ? provincesData[tentinh].fourPrizes : ['...', '...', '...', '...', '...', '...', '...'],
                        fivePrizes: provincesData[tentinh]?.fivePrizes.length ? provincesData[tentinh].fivePrizes : ['...'],
                        sixPrizes: provincesData[tentinh]?.sixPrizes.length ? provincesData[tentinh].sixPrizes : ['...', '...', '...'],
                        sevenPrizes: provincesData[tentinh]?.sevenPrizes.length ? provincesData[tentinh].sevenPrizes : ['...'],
                        eightPrizes: provincesData[tentinh]?.eightPrizes.length ? provincesData[tentinh].eightPrizes : ['...'],
                        station,
                        createdAt: new Date(),
                    };

                    const prizeTypes = [
                        { key: 'specialPrize', data: formattedResult.specialPrize, isArray: true },
                        { key: 'firstPrize', data: formattedResult.firstPrize, isArray: true },
                        { key: 'secondPrize', data: formattedResult.secondPrize, isArray: true },
                        { key: 'threePrizes', data: formattedResult.threePrizes, isArray: true },
                        { key: 'fourPrizes', data: formattedResult.fourPrizes, isArray: true },
                        { key: 'fivePrizes', data: formattedResult.fivePrizes, isArray: true },
                        { key: 'sixPrizes', data: formattedResult.sixPrizes, isArray: true },
                        { key: 'sevenPrizes', data: formattedResult.sevenPrizes, isArray: true },
                        { key: 'eightPrizes', data: formattedResult.eightPrizes, isArray: true },
                    ];

                    for (const { key, data, isArray } of prizeTypes) {
                        if (isArray) {
                            data.forEach((prize, index) => {
                                if (prize && prize !== '...') {
                                    const lastPrize = lastPrizeDataByProvince[tentinh][key][index] || '...';
                                    if (prize !== lastPrize) {
                                        console.log(`Kết quả mới cho ${key}_${index} (tỉnh ${tentinh}): ${prize}`);
                                        publishToRedis(`${key}_${index}`, prize, formattedResult);
                                        lastPrizeDataByProvince[tentinh][key][index] = prize;
                                    }
                                }
                            });
                        }
                    }

                    for (const { key, isArray } of prizeTypes) {
                        if (isArray && formattedResult[key].some(prize => prize !== '...')) {
                            console.log(`Publish mảng ${key} (tỉnh ${tentinh}):`, formattedResult[key]);
                            publishToRedis(key, formattedResult[key], formattedResult);
                        }
                    }

                    if (hasAnyData(formattedResult)) {
                        await saveToMongoDB(formattedResult);
                    } else {
                        console.log(`Dữ liệu ngày ${date} cho tỉnh ${tentinh} chưa có, tiếp tục cào...`);
                    }

                    await logPerformance(iterationStart, iteration, true, tentinh);

                    if (!isDataComplete(formattedResult)) {
                        allProvincesComplete = false;
                    }
                }

                successCount += 1;

                if (allProvincesComplete) {
                    console.log(`Dữ liệu ngày ${date} cho tất cả các tỉnh đã đầy đủ, dừng cào.`);
                    clearInterval(intervalId);
                    const totalDuration = (Date.now() - startTime) / 1000;
                    const stats = await pidusage(process.pid);
                    console.log('Tổng hiệu suất scraper:', {
                        totalDuration: `${totalDuration.toFixed(2)}s`,
                        cpu: `${stats.cpu.toFixed(2)}%`,
                        memory: `${(stats.memory / 1024 / 1024).toFixed(2)}MB`,
                        totalIterations: iteration,
                        successCount,
                        errorCount,
                    });
                    await page.close();
                    await browser.close();
                    await release();
                    return;
                }

                clearInterval(intervalId);
                intervalId = setInterval(scrapeAndSave, intervalMs);
            } catch (error) {
                console.error(`Lỗi khi cào dữ liệu ngày ${date}:`, error.message);
                await logPerformance(iterationStart, iteration, false, 'N/A');
                errorCount += 1;
            }
        };

        intervalId = setInterval(scrapeAndSave, 3000);

        setTimeout(async () => {
            clearInterval(intervalId);
            console.log(`Dừng cào dữ liệu ngày ${date} sau 23 phút`);
            const totalDuration = (Date.now() - startTime) / 1000;
            const stats = await pidusage(process.pid);
            console.log('Tổng hiệu suất scraper:', {
                totalDuration: `${totalDuration.toFixed(2)}s`,
                cpu: `${stats.cpu.toFixed(2)}%`,
                memory: `${(stats.memory / 1024 / 1024).toFixed(2)}MB`,
                totalIterations: iteration,
                successCount,
                errorCount,
            });
            if (page) await page.close();
            if (browser) await browser.close();
            if (release) await release();
        }, 17 * 60 * 1000);

    } catch (error) {
        console.error(`Lỗi khi khởi động scraper ngày ${date}:`, error.message);
        if (page) await page.close();
        if (browser) await browser.close();
        if (release) await release();
    }
}

module.exports = { scrapeXSMT };

// Lên lịch chạy tự động lúc 16h45 hàng ngày
schedule.scheduleJob('15 17 * * *', () => {
    console.log('Bắt đầu cào dữ liệu XSMT...');
    const today = new Date().toLocaleDateString('vi-VN', { day: '2-digit', month: '2-digit', year: 'numeric' });
    scrapeXSMT(today, 'xsmt');
});

// Chạy thủ công nếu có tham số dòng lệnh
const [, , date, station] = process.argv;
if (date && station) {
    console.log(`Chạy thủ công cho ngày ${date} và đài ${station}`);
    scrapeXSMT(date, station);
} else {
    console.log('Nếu muốn chạy thủ công, dùng lệnh: node xsmt_scraper.js 19/04/2025 xsmT');
}

// Đóng kết nối khi dừng chương trình
process.on('SIGINT', async () => {
    await mongoose.connection.close();
    await redisClient.quit();
    console.log('Đã đóng kết nối MongoDB và Redis');
    process.exit(0);
});