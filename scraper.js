const puppeteer = require('puppeteer');
const mongoose = require('mongoose');
const redis = require('redis');
const pidusage = require('pidusage');
const { connectMongoDB, isConnected } = require('./db');
require('dotenv').config();

process.env.TZ = 'Asia/Ho_Chi_Minh';

const XSMB = require('./src/models/XS_MB.models');

// Kết nối Redis
const redisClient = redis.createClient({
    url: process.env.REDIS_URL || 'redis://localhost:6379',
});
redisClient.connect().catch(err => console.error('Lỗi kết nối Redis:', err));

function formatDateToDDMMYYYY(date) {
    const day = String(date.getDate()).padStart(2, '0');
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const year = date.getFullYear();
    return `${day}-${month}-${year}`;
}

function isDataComplete(result, completedPrizes, stableCounts) {
    const checkPrize = (key, data, minLength) => {
        const isValid = Array.isArray(data) && data.length >= minLength && data.every(prize => prize && prize !== '...' && prize !== '****' && /^\d+$/.test(prize));
        stableCounts[key] = isValid ? (stableCounts[key] || 0) + 1 : 0;
        completedPrizes[key] = isValid && stableCounts[key] >= (key === 'specialPrize' ? 2 : 1);
        return isValid;
    };

    const isValidMaDB = result.maDB && typeof result.maDB === 'string' && result.maDB.trim() !== '' && result.maDB.trim() !== '...';
    stableCounts.maDB = isValidMaDB ? (stableCounts.maDB || 0) + 1 : 0;
    completedPrizes.maDB = isValidMaDB && stableCounts.maDB >= 1;

    checkPrize('firstPrize', result.firstPrize || [], 1);
    checkPrize('secondPrize', result.secondPrize || [], 2);
    checkPrize('threePrizes', result.threePrizes || [], 6);
    checkPrize('fourPrizes', result.fourPrizes || [], 4);
    checkPrize('fivePrizes', result.fivePrizes || [], 6);
    checkPrize('sixPrizes', result.sixPrizes || [], 3);
    checkPrize('sevenPrizes', result.sevenPrizes || [], 4);
    checkPrize('specialPrize', result.specialPrize || [], 1);

    const isComplete = completedPrizes.maDB && result.tentinh && result.tentinh.length >= 1 &&
        Object.keys(completedPrizes).every(k => completedPrizes[k]);
    if (isComplete) console.log('Dữ liệu hoàn thành');
    return isComplete;
}

async function publishToRedis(changes, additionalData) {
    const { drawDate, tentinh, tinh, year, month } = additionalData;
    const today = formatDateToDDMMYYYY(new Date(drawDate));
    try {
        if (!redisClient.isOpen) {
            console.log('Redis client chưa sẵn sàng, kết nối lại...');
            await redisClient.connect();
        }
        const pipeline = redisClient.multi();
        for (const { key, data } of changes) {
            pipeline.publish(`xsmb:${today}`, JSON.stringify({ prizeType: key, prizeData: data, drawDate: today, tentinh, tinh, year, month }));
            pipeline.hSet(`kqxs:${today}`, key, JSON.stringify(data));
        }
        pipeline.hSet(`kqxs:${today}:meta`, 'metadata', JSON.stringify({ tentinh, tinh, year, month }));
        await pipeline.exec();
        console.log(`Đã gửi ${changes.length} thay đổi qua Redis`);
    } catch (error) {
        console.error('Lỗi gửi Redis:', error.message);
        throw error;
    }
}

async function setRedisExpiration(today) {
    try {
        await Promise.all([
            redisClient.expire(`kqxs:${today}`, 7200),
            redisClient.expire(`kqxs:${today}:meta`, 7200),
        ]);
        console.log(`Đã đặt expire cho kqxs:${today} và metadata`);
    } catch (error) {
        console.error('Lỗi đặt expire Redis:', error.message);
    }
}

async function saveToMongoDB(result) {
    try {
        if (!isConnected()) {
            await connectMongoDB();
        }
        const existingResult = await XSMB.findOne({ drawDate: result.drawDate, station: result.station }).lean();
        if (existingResult) {
            const existingData = {
                firstPrize: existingResult.firstPrize,
                secondPrize: existingResult.secondPrize,
                threePrizes: existingResult.threePrizes,
                fourPrizes: existingResult.fourPrizes,
                fivePrizes: existingResult.fivePrizes,
                sixPrizes: existingResult.sixPrizes,
                sevenPrizes: existingResult.sevenPrizes,
                maDB: existingResult.maDB,
                specialPrize: existingResult.specialPrize,
            };
            const newData = {
                firstPrize: result.firstPrize,
                secondPrize: result.secondPrize,
                threePrizes: result.threePrizes,
                fourPrizes: result.fourPrizes,
                fivePrizes: result.fivePrizes,
                sixPrizes: result.sixPrizes,
                sevenPrizes: result.sevenPrizes,
                maDB: result.maDB,
                specialPrize: result.specialPrize,
            };
            if (JSON.stringify(existingData) !== JSON.stringify(newData)) {
                await XSMB.updateOne(
                    { drawDate: result.drawDate, station: result.station },
                    { $set: result },
                    { upsert: true }
                );
                console.log(`Cập nhật kết quả ngày ${result.drawDate.toISOString().split('T')[0]} cho ${result.station}`);
            }
        } else {
            await XSMB.create(result);
            console.log(`Lưu kết quả mới ngày ${result.drawDate.toISOString().split('T')[0]} cho ${result.station}`);
        }
    } catch (error) {
        console.error(`Lỗi khi lưu dữ liệu ngày ${result.drawDate.toISOString().split('T')[0]}:`, error.message);
    }
}

async function logPerformance(startTime, iteration, success) {
    if (iteration % 10 === 0 || !success) {
        const stats = await pidusage(process.pid);
        const duration = (Date.now() - startTime) / 1000;
        console.log(`Lần cào ${iteration} (${success ? 'Thành công' : 'Thất bại'}):`, {
            duration: `${duration.toFixed(2)}s`,
            cpu: `${stats.cpu.toFixed(2)}%`,
            memory: `${(stats.memory / 1024 / 1024).toFixed(2)}MB`,
        });
    }
}

async function scrapeXSMB(date, station, isTestMode = false) {
    let browser;
    let page;
    let intervalId;
    let isStopped = false;
    let iteration = 0;
    let successCount = 0;
    let errorCount = 0;
    const startTime = Date.now();
    const lastPrizeData = {
        firstPrize: ['...'],
        secondPrize: ['...', '...'],
        threePrizes: ['...', '...', '...', '...', '...', '...'],
        fourPrizes: ['...', '...', '...', '...'],
        fivePrizes: ['...', '...', '...', '...', '...', '...'],
        sixPrizes: ['...', '...', '...'],
        sevenPrizes: ['...', '...', '...', '...'],
        maDB: '...',
        specialPrize: ['...'],
    };
    const completedPrizes = {
        firstPrize: false,
        secondPrize: false,
        threePrizes: false,
        fourPrizes: false,
        fivePrizes: false,
        sixPrizes: false,
        sevenPrizes: false,
        maDB: false,
        specialPrize: false,
    };
    const stableCounts = {
        firstPrize: 0,
        secondPrize: 0,
        threePrizes: 0,
        fourPrizes: 0,
        fivePrizes: 0,
        sixPrizes: 0,
        sevenPrizes: 0,
        maDB: 0,
        specialPrize: 0,
    };

    try {
        const dateParts = date.split('/');
        const dateObj = new Date(`${dateParts[2]}-${dateParts[1]}-${dateParts[0]}`);
        if (isNaN(dateObj.getTime())) {
            throw new Error('Ngày không hợp lệ: ' + date);
        }
        const formattedDate = date.replace(/\//g, '-');

        const isLiveWindow = new Date().getHours() === 18 && new Date().getMinutes() >= 14 && new Date().getMinutes() <= 32;
        const intervalMs = isTestMode || isLiveWindow ? 1000 : 1000;
        console.log(`intervalMs: ${intervalMs}ms (isLiveWindow: ${isLiveWindow}, isTestMode: ${isTestMode})`);

        await connectMongoDB();

        browser = await puppeteer.launch({
            headless: true,
            args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
            executablePath: process.env.CHROMIUM_PATH || undefined,
        });
        page = await browser.newPage();
        await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124');

        let baseUrl, dateHash;
        if (station.toLowerCase() === 'xsmb') {
            baseUrl = `https://xosovn.com/xsmb-${formattedDate}`;
            dateHash = `#kqngay_${formattedDate.split('-').join('')}`;
            console.log(`Đang cào dữ liệu từ: ${baseUrl}`);
        } else {
            throw new Error('Chỉ hỗ trợ đài xsmb trong phiên bản này');
        }

        const selectors = {
            firstPrize: `${dateHash} span[class*="v-g1"]`,
            secondPrize: `${dateHash} span[class*="v-g2-"]`,
            threePrizes: `${dateHash} span[class*="v-g3-"]`,
            fourPrizes: `${dateHash} span[class*="v-g4-"]`,
            fivePrizes: `${dateHash} span[class*="v-g5-"]`,
            sixPrizes: `${dateHash} span[class*="v-g6-"]`,
            sevenPrizes: `${dateHash} span[class*="v-g7-"]`,
            maDB: `${dateHash} span[class*="v-madb"]:first-child`,
            specialPrize: `${dateHash} span[class*="v-gdb"]`,
        };

        const scrapeAndSave = async () => {
            if (isStopped || (page && page.isClosed())) {
                console.log(`Scraper đã dừng hoặc page đã đóng`);
                clearInterval(intervalId);
                return;
            }

            iteration += 1;
            const iterationStart = Date.now();
            console.log(`Bắt đầu lần cào ${iteration}`);

            try {
                if (iteration === 1) {
                    await page.goto(baseUrl, { waitUntil: 'domcontentloaded', timeout: 7000 });
                    await page.waitForSelector(`${dateHash} span[class*="v-madb"]`, { timeout: 2000 }).catch(() => {
                        console.log('Chưa thấy maDB, tiếp tục cào...');
                    });
                } else {
                    await page.waitForSelector(dateHash, { timeout: 2000 }).catch(() => {
                        console.log('Chưa thấy dateHash, tiếp tục cào...');
                    });
                }

                // Định nghĩa thứ tự cào cố định
                const prizeOrder = [
                    'firstPrize',
                    'secondPrize',
                    'threePrizes',
                    'fourPrizes',
                    'fivePrizes',
                    'sixPrizes',
                    'sevenPrizes',
                    'maDB',
                    'specialPrize',
                ].filter(key => !completedPrizes[key]);

                const result = await page.evaluate(({ dateHash, selectors, prizeOrder }) => {
                    const getPrizes = (selector) => {
                        try {
                            const elements = document.querySelectorAll(selector);
                            return Array.from(elements)
                                .map(elem => elem.getAttribute('data-id')?.trim() || '')
                                .filter(prize => prize && prize !== '...' && prize !== '****' && /^\d+$/.test(prize));
                        } catch (error) {
                            console.error(`Lỗi lấy selector ${selector}:`, error.message);
                            return [];
                        }
                    };

                    const result = { drawDate: document.querySelector('.ngay_quay')?.textContent.trim() || '' };
                    for (const prizeType of prizeOrder) {
                        if (prizeType === 'maDB') {
                            const maDBElement = document.querySelector(selectors.maDB);
                            result.maDB = maDBElement ? maDBElement.textContent.trim() : '...';
                        } else {
                            result[prizeType] = getPrizes(selectors[prizeType]) || [];
                        }
                    }
                    return result;
                }, { dateHash, selectors, prizeOrder });

                const dayOfWeekIndex = dateObj.getDay();
                let tinh, tentinh;
                switch (dayOfWeekIndex) {
                    case 0: tinh = 'thai-binh'; tentinh = 'Thái Bình'; break;
                    case 1: tinh = 'ha-noi'; tentinh = 'Hà Nội'; break;
                    case 2: tinh = 'quang-ninh'; tentinh = 'Quảng Ninh'; break;
                    case 3: tinh = 'bac-ninh'; tentinh = 'Bắc Ninh'; break;
                    case 4: tinh = 'ha-noi'; tentinh = 'Hà Nội'; break;
                    case 5: tinh = 'hai-phong'; tentinh = 'Hải Phòng'; break;
                    case 6: tinh = 'nam-dinh'; tentinh = 'Nam Định'; break;
                    default: tinh = 'ha-noi'; tentinh = 'Hà Nội';
                }

                const slug = `${station}-${formattedDate}`;
                const dayOfWeek = ['Chủ nhật', 'Thứ 2', 'Thứ 3', 'Thứ 4', 'Thứ 5', 'Thứ 6', 'Thứ 7'][dayOfWeekIndex] || 'Thứ 2';

                const formattedResult = {
                    drawDate: dateObj,
                    slug,
                    year: dateObj.getFullYear(),
                    month: dateObj.getMonth() + 1,
                    dayOfWeek,
                    maDB: result.maDB || lastPrizeData.maDB,
                    tentinh,
                    tinh,
                    firstPrize: Array.isArray(result.firstPrize) && result.firstPrize.length ? result.firstPrize : lastPrizeData.firstPrize,
                    secondPrize: Array.isArray(result.secondPrize) && result.secondPrize.length ? result.secondPrize : lastPrizeData.secondPrize,
                    threePrizes: Array.isArray(result.threePrizes) && result.threePrizes.length ? result.threePrizes : lastPrizeData.threePrizes,
                    fourPrizes: Array.isArray(result.fourPrizes) && result.fourPrizes.length ? result.fourPrizes : lastPrizeData.fourPrizes,
                    fivePrizes: Array.isArray(result.fivePrizes) && result.fivePrizes.length ? result.fivePrizes : lastPrizeData.fivePrizes,
                    sixPrizes: Array.isArray(result.sixPrizes) && result.sixPrizes.length ? result.sixPrizes : lastPrizeData.sixPrizes,
                    sevenPrizes: Array.isArray(result.sevenPrizes) && result.sevenPrizes.length ? result.sevenPrizes : lastPrizeData.sevenPrizes,
                    specialPrize: Array.isArray(result.specialPrize) && result.specialPrize.length ? result.specialPrize : lastPrizeData.specialPrize,
                    station,
                    createdAt: new Date(),
                };

                const prizeTypes = [
                    { key: 'firstPrize', data: formattedResult.firstPrize, isArray: true, minLength: 1 },
                    { key: 'secondPrize', data: formattedResult.secondPrize, isArray: true, minLength: 2 },
                    { key: 'threePrizes', data: formattedResult.threePrizes, isArray: true, minLength: 6 },
                    { key: 'fourPrizes', data: formattedResult.fourPrizes, isArray: true, minLength: 4 },
                    { key: 'fivePrizes', data: formattedResult.fivePrizes, isArray: true, minLength: 6 },
                    { key: 'sixPrizes', data: formattedResult.sixPrizes, isArray: true, minLength: 3 },
                    { key: 'sevenPrizes', data: formattedResult.sevenPrizes, isArray: true, minLength: 4 },
                    { key: 'maDB', data: formattedResult.maDB, isArray: false, minLength: 1 },
                    { key: 'specialPrize', data: formattedResult.specialPrize, isArray: true, minLength: 1 },
                ];

                const changes = [];
                for (const { key, data, isArray, minLength } of prizeTypes) {
                    if (isArray) {
                        if (!Array.isArray(data)) {
                            console.warn(`Dữ liệu ${key} không phải mảng, bỏ qua`);
                            continue;
                        }
                        for (const [index, prize] of data.entries()) {
                            if (prize && prize !== '...' && prize !== '****' && /^\d+$/.test(prize) && prize !== lastPrizeData[key][index]) {
                                changes.push({ key: `${key}_${index}`, data: prize });
                                lastPrizeData[key][index] = prize;
                            }
                        }
                    } else if (data && data !== '...' && data !== '' && data !== lastPrizeData[key]) {
                        changes.push({ key, data });
                        lastPrizeData[key] = data;
                    }
                }

                if (changes.length) {
                    await publishToRedis(changes, formattedResult);
                }

                formattedResult.firstPrize = lastPrizeData.firstPrize;
                formattedResult.secondPrize = lastPrizeData.secondPrize;
                formattedResult.threePrizes = lastPrizeData.threePrizes;
                formattedResult.fourPrizes = lastPrizeData.fourPrizes;
                formattedResult.fivePrizes = lastPrizeData.fivePrizes;
                formattedResult.sixPrizes = lastPrizeData.sixPrizes;
                formattedResult.sevenPrizes = lastPrizeData.sevenPrizes;
                formattedResult.maDB = lastPrizeData.maDB;
                formattedResult.specialPrize = lastPrizeData.specialPrize;

                if (isDataComplete(formattedResult, completedPrizes, stableCounts)) {
                    console.log(`Dữ liệu ngày ${date} cho ${station} đã đầy đủ, dừng cào.`);
                    isStopped = true;
                    clearInterval(intervalId);
                    await saveToMongoDB(formattedResult);
                    await setRedisExpiration(formatDateToDDMMYYYY(dateObj));

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

                    if (page && !page.isClosed()) await page.close();
                    if (browser) await browser.close();
                    return;
                }

                await logPerformance(iterationStart, iteration, true);
                successCount += 1;
            } catch (error) {
                console.error(`Lỗi khi cào dữ liệu ngày ${date}:`, error.message);
                await logPerformance(iterationStart, iteration, false);
                errorCount += 1;
            }
        };

        await scrapeAndSave();
        if (!isStopped) {
            intervalId = setInterval(scrapeAndSave, intervalMs);
        }

        setTimeout(async () => {
            if (!isStopped) {
                isStopped = true;
                clearInterval(intervalId);
                console.log(`Dữ liệu ngày ${date} cho ${station} dừng sau 20 phút.`);
                await saveToMongoDB(formattedResult);
                await setRedisExpiration(formatDateToDDMMYYYY(dateObj));

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

                if (page && !page.isClosed()) await page.close();
                if (browser) await browser.close();
            }
        }, 20 * 60 * 1000);

    } catch (error) {
        console.error(`Lỗi khi khởi động scraper ngày ${date}:`, error.message);
        isStopped = true;
        await setRedisExpiration(formatDateToDDMMYYYY(dateObj || new Date()));
        if (page && !page.isClosed()) await page.close();
        if (browser) await browser.close();
    }
}

module.exports = { scrapeXSMB };

const [, , date, station, testMode] = process.argv;
if (date && station) {
    const isTestMode = testMode === 'test';
    console.log(`Chạy thủ công cho ngày ${date} và đài ${station}${isTestMode ? ' (chế độ thử nghiệm)' : ''}`);
    scrapeXSMB(date, station, isTestMode);
} else {
    console.log('Chạy thủ công: node scraper.js 24/01/2025 xsmb [test]');
}

process.on('SIGINT', async () => {
    await redisClient.quit();
    console.log('Đã đóng kết nối Redis MIỀN BẮC');
    process.exit(0);
});
// đã cào ổn, chỉ bị mỗi maDB chưa sửa.(phiên bản hiện tại đang sử dụng đã sửa) : phiên bản này chưa 19/6