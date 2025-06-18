const puppeteer = require('puppeteer');
const mongoose = require('mongoose');
const redis = require('redis');
const { lock } = require('proper-lockfile');
const fs = require('fs');
const path = require('path');
const pidusage = require('pidusage');
require('dotenv').config();

const XSMT = require('./src/models/XS_MT.models');

// Kết nối MongoDB
let mongooseConnected = false;
async function connectMongoDB() {
    if (mongooseConnected || mongoose.connection.readyState === 1) return;
    try {
        await mongoose.connect(process.env.MONGODB_URI || 'mongodb://localhost:27017/xsmt', {
            maxPoolSize: 5,
            minPoolSize: 1,
        });
        mongooseConnected = true;
        console.log('Đã kết nối MongoDB');
    } catch (err) {
        console.error('Lỗi kết nối MongoDB:', err.message);
        throw err;
    }
}

// Kết nối Redis
const redisClient = redis.createClient({
    url: process.env.REDIS_URL || 'redis://localhost:6379',
});
redisClient.connect().catch(err => console.error('Lỗi kết nối Redis:', err.message));

// File lock
const lockFilePath = path.resolve(__dirname, 'xsmt_scraper.lock');
const ensureLockFile = () => {
    if (!fs.existsSync(lockFilePath)) {
        fs.writeFileSync(lockFilePath, '');
        console.log(`Tạo file ${lockFilePath}`);
    }
};

// Xóa lock file cũ
const clearStaleLock = async () => {
    try {
        if (fs.existsSync(lockFilePath)) {
            const stats = fs.statSync(lockFilePath);
            if (Date.now() - new Date(stats.mtime).getTime() > 10000) {
                fs.unlinkSync(lockFilePath);
                console.log(`Đã xóa file lock cũ: ${lockFilePath}`);
            }
        }
    } catch (error) {
        console.error('Lỗi khi xóa file lock:', error.message);
    }
};

// Chuyển đổi tên tỉnh sang kebab-case
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

// Định dạng ngày thành DD-MM-YYYY
function formatDateToDDMMYYYY(date) {
    const day = String(date.getDate()).padStart(2, '0');
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const year = date.getFullYear();
    return `${day}-${month}-${year}`;
}

// Log chi tiết dữ liệu
function logDataDetails(result) {
    const expectedCounts = [
        { key: 'eightPrizes', actual: result.eightPrizes?.length || 0, expected: 1 },
        { key: 'sevenPrizes', actual: result.sevenPrizes?.length || 0, expected: 1 },
        { key: 'sixPrizes', actual: result.sixPrizes?.length || 0, expected: 3 },
        { key: 'fivePrizes', actual: result.fivePrizes?.length || 0, expected: 1 },
        { key: 'fourPrizes', actual: result.fourPrizes?.length || 0, expected: 7 },
        { key: 'threePrizes', actual: result.threePrizes?.length || 0, expected: 2 },
        { key: 'secondPrize', actual: result.secondPrize?.length || 0, expected: 1 },
        { key: 'firstPrize', actual: result.firstPrize?.length || 0, expected: 1 },
        { key: 'specialPrize', actual: result.specialPrize?.length || 0, expected: 1 },
    ];

    console.log(`Chi tiết dữ liệu cho tỉnh ${result.tentinh}:`);
    for (const { key, actual, expected } of expectedCounts) {
        console.log(`- ${key}: ${actual}/${expected}${actual < expected ? ' (THIẾU)' : ''}`);
    }
}

// Kiểm tra dữ liệu đầy đủ
function isDataComplete(result, completedPrizes, stableCounts) {
    const checkPrize = (key, data, minLength) => {
        const isValid = Array.isArray(data) && data.length >= minLength && data.every(prize => prize && prize !== '...' && /^\d+$/.test(prize));
        stableCounts[key] = isValid ? (stableCounts[key] || 0) + 1 : 0;
        completedPrizes[key] = isValid && stableCounts[key] >= (key === 'specialPrize' ? 2 : 1);
        return isValid;
    };

    checkPrize('eightPrizes', result.eightPrizes, 1);
    checkPrize('sevenPrizes', result.sevenPrizes, 1);
    checkPrize('sixPrizes', result.sixPrizes, 3);
    checkPrize('fivePrizes', result.fivePrizes, 1);
    checkPrize('fourPrizes', result.fourPrizes, 7);
    checkPrize('threePrizes', result.threePrizes, 2);
    checkPrize('secondPrize', result.secondPrize, 1);
    checkPrize('firstPrize', result.firstPrize, 1);
    checkPrize('specialPrize', result.specialPrize, 1);

    return result.tentinh && result.tentinh.length >= 1 && !result.tentinh.startsWith('Tỉnh_') &&
        Object.keys(completedPrizes).every(k => completedPrizes[k]);
}

// Kiểm tra có dữ liệu đáng lưu
function hasAnyData(result) {
    return (
        result.tentinh && result.tentinh.length >= 1 && !result.tentinh.startsWith('Tỉnh_') &&
        (
            (result.eightPrizes && result.eightPrizes.length >= 1 && result.eightPrizes.every(p => p && p !== '...')) ||
            (result.sevenPrizes && result.sevenPrizes.length >= 1 && result.sevenPrizes.every(p => p && p !== '...')) ||
            (result.sixPrizes && result.sixPrizes.length >= 1 && result.sixPrizes.every(p => p && p !== '...')) ||
            (result.fivePrizes && result.fivePrizes.length >= 1 && result.fivePrizes.every(p => p && p !== '...')) ||
            (result.fourPrizes && result.fourPrizes.length >= 1 && result.fourPrizes.every(p => p && p !== '...')) ||
            (result.threePrizes && result.threePrizes.length >= 1 && result.threePrizes.every(p => p && p !== '...')) ||
            (result.secondPrize && result.secondPrize.length >= 1 && result.secondPrize.every(p => p && p !== '...')) ||
            (result.firstPrize && result.firstPrize.length >= 1 && result.firstPrize.every(p => p && p !== '...')) ||
            (result.specialPrize && result.specialPrize.length >= 1 && result.specialPrize.every(p => p && p !== '...'))
        )
    );
}

// Publish dữ liệu lên Redis
async function publishToRedis(changes, additionalData) {
    const { drawDate, tentinh, tinh, year, month } = additionalData;
    const today = formatDateToDDMMYYYY(new Date(drawDate));
    try {
        if (!redisClient.isOpen) {
            await redisClient.connect();
        }
        const pipeline = redisClient.multi();
        for (const { key, data } of changes) {
            pipeline.publish(`xsmt:${today}:${tinh}`, JSON.stringify({ prizeType: key, prizeData: data, drawDate: today, tentinh, tinh, year, month }));
            pipeline.hSet(`kqxs:xsmt:${today}:${tinh}`, key, JSON.stringify(data));
        }
        pipeline.hSet(`kqxs:xsmt:${today}:${tinh}:meta`, 'metadata', JSON.stringify({ tentinh, tinh, year, month }));
        pipeline.expire(`kqxs:xsmt:${today}:${tinh}`, 7200);
        pipeline.expire(`kqxs:xsmt:${today}:${tinh}:meta`, 7200);
        await pipeline.exec();
        console.log(`Đã gửi ${changes.length} thay đổi qua Redis cho tỉnh ${tentinh}`);
    } catch (error) {
        console.error(`Lỗi gửi Redis (tỉnh ${tentinh}):`, error.message);
    }
}

// Lưu dữ liệu vào MongoDB
async function saveToMongoDB(result) {
    try {
        const dateObj = new Date(result.drawDate);
        const existingResult = await XSMT.findOne({ drawDate: dateObj, station: result.station, tentinh: result.tentinh }).lean();

        if (existingResult) {
            const existingData = {
                eightPrizes: existingResult.eightPrizes,
                sevenPrizes: existingResult.sevenPrizes,
                sixPrizes: existingResult.sixPrizes,
                fivePrizes: existingResult.fivePrizes,
                fourPrizes: existingResult.fourPrizes,
                threePrizes: existingResult.threePrizes,
                secondPrize: existingResult.secondPrize,
                firstPrize: existingResult.firstPrize,
                specialPrize: existingResult.specialPrize,
            };

            const newData = {
                eightPrizes: result.eightPrizes,
                sevenPrizes: result.sevenPrizes,
                sixPrizes: result.sixPrizes,
                fivePrizes: result.fivePrizes,
                fourPrizes: result.fourPrizes,
                threePrizes: result.threePrizes,
                secondPrize: result.secondPrize,
                firstPrize: result.firstPrize,
                specialPrize: result.specialPrize,
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

// Log hiệu suất
async function logPerformance(startTime, iteration, success, province) {
    if (iteration % 10 === 0 || !success) {
        const stats = await pidusage(process.pid);
        const duration = (Date.now() - startTime) / 1000;
        console.log(`Lần cào ${iteration} cho tỉnh ${province} (${success ? 'Thành công' : 'Thất bại'}):`, {
            duration: `${duration.toFixed(2)}s`,
            cpu: `${stats.cpu.toFixed(2)}%`,
            memory: `${(stats.memory / 1024 / 1024).toFixed(2)}MB`,
        });
    }
}

// Hàm cào dữ liệu XSMT
async function scrapeXSMT(date, station) {
    let browser;
    let page;
    let intervalId;
    let release;
    let isStopped = false;
    let iteration = 0;
    let successCount = 0;
    let errorCount = 0;
    const startTime = Date.now();
    const lastPrizeDataByProvince = {};

    try {
        const dateParts = date.split('/');
        const dateObj = new Date(`${dateParts[2]}-${dateParts[1]}-${dateParts[0]}`);
        if (isNaN(dateObj.getTime())) {
            throw new Error('Ngày không hợp lệ: ' + date);
        }
        const formattedDate = date.replace(/\//g, '-');

        await clearStaleLock();
        await connectMongoDB();
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
            if (['image', 'stylesheet', 'font', 'media'].includes(req.resourceType())) {
                req.abort();
            } else {
                req.continue();
            }
        });
        page.on('framenavigated', () => {
            console.log('Page navigated, stopping scraper...');
            isStopped = true;
        });

        let baseUrl;
        if (station.toLowerCase() === 'xsmt') {
            baseUrl = `https://xosovn.com/xsmt-${formattedDate}`;
            console.log(`Đang cào dữ liệu từ: ${baseUrl}`);
        } else {
            throw new Error('Chỉ hỗ trợ đài xsmt trong phiên bản này');
        }

        const selectors = {
            eightPrizes: 'span[class*="v-g8"]',
            sevenPrizes: 'span[class*="v-g7"]',
            sixPrizes: 'span[class*="v-g6-"]',
            fivePrizes: 'span[class*="v-g5"]',
            fourPrizes: 'span[class*="v-g4-"]',
            threePrizes: 'span[class*="v-g3-"]',
            secondPrize: 'span[class*="v-g2"]',
            firstPrize: 'span[class*="v-g1"]',
            specialPrize: 'span[class*="v-gdb"]',
        };

        const scrapeAndSave = async () => {
            if (isStopped || (page && page.isClosed())) {
                clearInterval(intervalId);
                return;
            }

            iteration += 1;
            const iterationStart = Date.now();
            console.log(`Bắt đầu lần cào ${iteration}`);

            try {
                const isLiveWindow = new Date().getHours() === 17 && new Date().getMinutes() >= 10 && new Date().getMinutes() <= 33;
                const intervalMs = isLiveWindow ? 2000 : 10000;

                if (page.isClosed()) {
                    page = await browser.newPage();
                    await page.setRequestInterception(true);
                    page.on('request', (req) => {
                        if (['image', 'stylesheet', 'font', 'media'].includes(req.resourceType())) {
                            req.abort();
                        } else {
                            req.continue();
                        }
                    });
                    page.on('framenavigated', () => {
                        isStopped = true;
                    });
                }

                const response = await page.goto(baseUrl, { waitUntil: 'networkidle2', timeout: 30000 });
                if (response.status() >= 400) {
                    throw new Error(`Lỗi HTTP ${response.status()}`);
                }
                await page.waitForFunction(
                    () => document.querySelectorAll('span[class*="v-gdb"]').length > 0 && Array.from(document.querySelectorAll('span[class*="v-gdb"]')).some(span => span.getAttribute('data-id')?.trim()),
                    { timeout: 30000 }
                );

                const result = await page.evaluate(({ selectors }) => {
                    const getPrizes = (selector) => {
                        const elements = document.querySelectorAll(selector);
                        return Array.from(elements)
                            .map(elem => elem.getAttribute('data-id')?.trim() || '')
                            .filter(prize => prize && prize !== '...' && /^\d+$/.test(prize));
                    };

                    const provinces = [];
                    const provinceRow = document.querySelector('table.kqsx-mt tr.bg-pr');
                    if (!provinceRow) {
                        return { provinces, provincesData: {}, drawDate: '' };
                    }
                    provinceRow.querySelectorAll('th').forEach((elem, i) => {
                        if (i === 0) return;
                        const provinceName = elem.querySelector('a')?.textContent.trim();
                        if (provinceName && !provinceName.startsWith('Tỉnh_')) {
                            provinces.push(provinceName);
                        }
                    });
                    if (provinces.length === 0) {
                        return { provinces, provincesData: {}, drawDate: '' };
                    }

                    const provincesData = {};
                    provinces.forEach(province => {
                        provincesData[province] = {
                            eightPrizes: [],
                            sevenPrizes: [],
                            sixPrizes: [],
                            fivePrizes: [],
                            fourPrizes: [],
                            threePrizes: [],
                            secondPrize: [],
                            firstPrize: [],
                            specialPrize: [],
                        };
                    });

                    const resultTable = document.querySelector('table.kqsx-mt');
                    if (resultTable) {
                        resultTable.querySelectorAll('tr').forEach((row) => {
                            if (row.className === 'bg-pr') return;
                            row.querySelectorAll('td').forEach((cell, j) => {
                                if (j === 0) return;
                                const province = provinces[j - 1];
                                if (!province) return;

                                Object.entries(selectors).forEach(([prizeType, selector]) => {
                                    const spans = cell.querySelectorAll(selector);
                                    spans.forEach(span => {
                                        const prize = span.getAttribute('data-id')?.trim();
                                        if (prize && prize !== '...' && /^\d+$/.test(prize)) {
                                            provincesData[province][prizeType].push(prize);
                                        }
                                    });
                                });
                            });
                        });
                    }

                    const drawDate = document.querySelector('.ngay_quay, .draw-date, .date, h1.df-title')?.textContent.trim().match(/\d{2}\/\d{2}\/\d{4}/)?.[0] || '';
                    return { provinces, provincesData, drawDate };
                }, { selectors });

                if (result.provinces.length === 0) {
                    await logPerformance(iterationStart, iteration, false, 'N/A');
                    errorCount += 1;
                    return;
                }

                result.provinces.forEach(province => {
                    if (!lastPrizeDataByProvince[province]) {
                        lastPrizeDataByProvince[province] = {
                            eightPrizes: ['...'],
                            sevenPrizes: ['...'],
                            sixPrizes: ['...', '...', '...'],
                            fivePrizes: ['...'],
                            fourPrizes: ['...', '...', '...', '...', '...', '...', '...'],
                            threePrizes: ['...', '...'],
                            secondPrize: ['...'],
                            firstPrize: ['...'],
                            specialPrize: ['...'],
                        };
                        lastPrizeDataByProvince[province].completedPrizes = {
                            eightPrizes: false,
                            sevenPrizes: false,
                            sixPrizes: false,
                            fivePrizes: false,
                            fourPrizes: false,
                            threePrizes: false,
                            secondPrize: false,
                            firstPrize: false,
                            specialPrize: false,
                        };
                        lastPrizeDataByProvince[province].stableCounts = {
                            eightPrizes: 0,
                            sevenPrizes: 0,
                            sixPrizes: 0,
                            fivePrizes: 0,
                            fourPrizes: 0,
                            threePrizes: 0,
                            secondPrize: 0,
                            firstPrize: 0,
                            specialPrize: 0,
                        };
                    }
                });

                const drawDate = result.drawDate || date;
                const dateObj = new Date(drawDate.split('/').reverse().join('-'));
                const daysOfWeek = ['Chủ nhật', 'Thứ 2', 'Thứ 3', 'Thứ 4', 'Thứ 5', 'Thứ 6', 'Thứ 7'];
                const dayOfWeek = daysOfWeek[dateObj.getDay()];

                let allProvincesComplete = true;

                for (const tentinh of result.provinces) {
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
                        eightPrizes: result.provincesData[tentinh]?.eightPrizes.length ? result.provincesData[tentinh].eightPrizes : lastPrizeDataByProvince[tentinh].eightPrizes,
                        sevenPrizes: result.provincesData[tentinh]?.sevenPrizes.length ? result.provincesData[tentinh].sevenPrizes : lastPrizeDataByProvince[tentinh].sevenPrizes,
                        sixPrizes: result.provincesData[tentinh]?.sixPrizes.length ? result.provincesData[tentinh].sixPrizes : lastPrizeDataByProvince[tentinh].sixPrizes,
                        fivePrizes: result.provincesData[tentinh]?.fivePrizes.length ? result.provincesData[tentinh].fivePrizes : lastPrizeDataByProvince[tentinh].fivePrizes,
                        fourPrizes: result.provincesData[tentinh]?.fourPrizes.length ? result.provincesData[tentinh].fourPrizes : lastPrizeDataByProvince[tentinh].fourPrizes,
                        threePrizes: result.provincesData[tentinh]?.threePrizes.length ? result.provincesData[tentinh].threePrizes : lastPrizeDataByProvince[tentinh].threePrizes,
                        secondPrize: result.provincesData[tentinh]?.secondPrize.length ? result.provincesData[tentinh].secondPrize : lastPrizeDataByProvince[tentinh].secondPrize,
                        firstPrize: result.provincesData[tentinh]?.firstPrize.length ? result.provincesData[tentinh].firstPrize : lastPrizeDataByProvince[tentinh].firstPrize,
                        specialPrize: result.provincesData[tentinh]?.specialPrize.length ? result.provincesData[tentinh].specialPrize : lastPrizeDataByProvince[tentinh].specialPrize,
                        station,
                        createdAt: new Date(),
                    };

                    logDataDetails(formattedResult);

                    const prizeTypes = [
                        { key: 'eightPrizes', data: formattedResult.eightPrizes, isArray: true, minLength: 1 },
                        { key: 'sevenPrizes', data: formattedResult.sevenPrizes, isArray: true, minLength: 1 },
                        { key: 'sixPrizes', data: formattedResult.sixPrizes, isArray: true, minLength: 3 },
                        { key: 'fivePrizes', data: formattedResult.fivePrizes, isArray: true, minLength: 1 },
                        { key: 'fourPrizes', data: formattedResult.fourPrizes, isArray: true, minLength: 7 },
                        { key: 'threePrizes', data: formattedResult.threePrizes, isArray: true, minLength: 2 },
                        { key: 'secondPrize', data: formattedResult.secondPrize, isArray: true, minLength: 1 },
                        { key: 'firstPrize', data: formattedResult.firstPrize, isArray: true, minLength: 1 },
                        { key: 'specialPrize', data: formattedResult.specialPrize, isArray: true, minLength: 1 },
                    ];

                    const changes = [];
                    for (const { key, data, isArray, minLength } of prizeTypes) {
                        if (isArray && Array.isArray(data)) {
                            data.forEach((prize, index) => {
                                if (prize && prize !== '...' && /^\d+$/.test(prize) && prize !== lastPrizeDataByProvince[tentinh][key][index]) {
                                    changes.push({ key: `${key}_${index}`, data: prize });
                                    lastPrizeDataByProvince[tentinh][key][index] = prize;
                                }
                            });
                            if (data.length >= minLength && data.every(p => p && p !== '...' && /^\d+$/.test(p))) {
                                changes.push({ key, data });
                            }
                        }
                    }

                    if (changes.length) {
                        await publishToRedis(changes, formattedResult);
                    }

                    formattedResult.eightPrizes = lastPrizeDataByProvince[tentinh].eightPrizes;
                    formattedResult.sevenPrizes = lastPrizeDataByProvince[tentinh].sevenPrizes;
                    formattedResult.sixPrizes = lastPrizeDataByProvince[tentinh].sixPrizes;
                    formattedResult.fivePrizes = lastPrizeDataByProvince[tentinh].fivePrizes;
                    formattedResult.fourPrizes = lastPrizeDataByProvince[tentinh].fourPrizes;
                    formattedResult.threePrizes = lastPrizeDataByProvince[tentinh].threePrizes;
                    formattedResult.secondPrize = lastPrizeDataByProvince[tentinh].secondPrize;
                    formattedResult.firstPrize = lastPrizeDataByProvince[tentinh].firstPrize;
                    formattedResult.specialPrize = lastPrizeDataByProvince[tentinh].specialPrize;

                    if (hasAnyData(formattedResult)) {
                        await saveToMongoDB(formattedResult);
                    }

                    if (!isDataComplete(formattedResult, lastPrizeDataByProvince[tentinh].completedPrizes, lastPrizeDataByProvince[tentinh].stableCounts)) {
                        allProvincesComplete = false;
                    }
                }

                successCount += 1;
                await logPerformance(iterationStart, iteration, true, 'All');

                if (allProvincesComplete) {
                    console.log(`Dữ liệu ngày ${date} cho tất cả các tỉnh đã đầy đủ, dừng cào.`);
                    isStopped = true;
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
                    if (!page.isClosed()) await page.close();
                    await browser.close();
                    if (release) await release();
                    return;
                }

                clearInterval(intervalId);
                intervalId = setInterval(scrapeAndSave, intervalMs);
            } catch (error) {
                errorCount += 1;
                await logPerformance(iterationStart, iteration, false, 'N/A');
            }
        };

        await scrapeAndSave();

        setTimeout(async () => {
            if (!isStopped) {
                isStopped = true;
                clearInterval(intervalId);
                console.log(`Dừng cào dữ liệu ngày ${date} sau 17 phút`);
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
                if (!page.isClosed()) await page.close();
                await browser.close();
                if (release) await release();
            }
        }, 17 * 60 * 1000);

    } catch (error) {
        console.error(`Lỗi khi khởi động scraper ngày ${date}:`, error.message);
        isStopped = true;
        clearInterval(intervalId);
        if (page && !page.isClosed()) await page.close();
        if (browser) await browser.close();
        if (release) await release();
    } finally {
        if (mongooseConnected) {
            await mongoose.connection.close();
            mongooseConnected = false;
            console.log('Đã đóng kết nối MongoDB');
        }
    }
}

module.exports = { scrapeXSMT };

// Chạy thủ công
const [, , date, station] = process.argv;
if (date && station) {
    console.log(`Chạy thủ công cho ngày ${date} và đài ${station}`);
    scrapeXSMT(date, station);
} else {
    console.log('Dùng lệnh: node xsmt_scraper.js 19/04/2025 xsmt');
}

// Đóng kết nối khi dừng
process.on('SIGINT', async () => {
    if (mongooseConnected) {
        await mongoose.connection.close();
        mongooseConnected = false;
        console.log('Đã đóng kết nối MongoDB');
    }
    await redisClient.quit();
    console.log('Đã đóng kết nối Redis');
    process.exit(0);
});