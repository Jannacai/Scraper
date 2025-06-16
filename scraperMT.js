const puppeteer = require('puppeteer');
const mongoose = require('mongoose');
const redis = require('redis');
const { lock } = require('proper-lockfile');
const fs = require('fs');
const path = require('path');
const pidusage = require('pidusage');
require('dotenv').config();

const XSMT = require('./src/models/XS_MT.models');

// Kết nối MongoDB với kiểm tra trạng thái
let mongooseConnected = false;
async function connectMongoDB() {
    if (mongooseConnected || mongoose.connection.readyState === 1) {
        console.log('MongoDB already connected');
        return;
    }
    try {
        await mongoose.connect(process.env.MONGODB_URI || 'mongodb://localhost:27017/xsmt', {
            maxPoolSize: 20,
            minPoolSize: 2,
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
function isDataComplete(result, completedPrizes, stableCounts) {
    const checkPrize = (key, data, minLength) => {
        const isValid = Array.isArray(data) && data.length >= minLength && data.every(prize => prize && prize !== '...' && /^\d+$/.test(prize));
        stableCounts[key] = isValid ? (stableCounts[key] || 0) + 1 : 0;
        completedPrizes[key] = isValid && stableCounts[key] >= (key === 'specialPrize' ? 2 : 1);
        return isValid;
    };

    checkPrize('specialPrize', result.specialPrize, 1);
    checkPrize('firstPrize', result.firstPrize, 1);
    checkPrize('secondPrize', result.secondPrize, 1);
    checkPrize('threePrizes', result.threePrizes, 2);
    checkPrize('fourPrizes', result.fourPrizes, 7);
    checkPrize('fivePrizes', result.fivePrizes, 1);
    checkPrize('sixPrizes', result.sixPrizes, 3);
    checkPrize('sevenPrizes', result.sevenPrizes, 1);
    checkPrize('eightPrizes', result.eightPrizes, 1);

    return result.tentinh && result.tentinh.length >= 1 && !result.tentinh.startsWith('Tỉnh_') &&
        Object.keys(completedPrizes).every(k => completedPrizes[k]);
}

// Hàm kiểm tra xem có dữ liệu nào đáng lưu không
function hasAnyData(result) {
    return (
        result.tentinh && result.tentinh.length >= 1 && !result.tentinh.startsWith('Tỉnh_') &&
        (
            (result.specialPrize && result.specialPrize.length >= 1 && result.specialPrize.every(p => p && p !== '...')) ||
            (result.firstPrize && result.firstPrize.length >= 1 && result.firstPrize.every(p => p && p !== '...')) ||
            (result.secondPrize && result.secondPrize.length >= 1 && result.secondPrize.every(p => p && p !== '...')) ||
            (result.threePrizes && result.threePrizes.length >= 1 && result.threePrizes.every(p => p && p !== '...')) ||
            (result.fourPrizes && result.fourPrizes.length >= 1 && result.fourPrizes.every(p => p && p !== '...')) ||
            (result.fivePrizes && result.fivePrizes.length >= 1 && result.fivePrizes.every(p => p && p !== '...')) ||
            (result.sixPrizes && result.sixPrizes.length >= 1 && result.sixPrizes.every(p => p && p !== '...')) ||
            (result.sevenPrizes && result.sevenPrizes.length >= 1 && result.sevenPrizes.every(p => p && p !== '...')) ||
            (result.eightPrizes && result.eightPrizes.length >= 1 && result.eightPrizes.every(p => p && p !== '...'))
        )
    );
}

// Hàm publish dữ liệu lên Redis
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
        const dateHash = `#kqngay_${formattedDate.split('-').join('')}`;

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

        const selectors = {
            specialPrize: `${dateHash} span[class*="v-gdb"]`,
            firstPrize: `${dateHash} span[class*="v-g1"]`,
            secondPrize: `${dateHash} span[class*="v-g2"]`,
            threePrizes: `${dateHash} span[class*="v-g3-"]`,
            fourPrizes: `${dateHash} span[class*="v-g4-"]`,
            fivePrizes: `${dateHash} span[class*="v-g5"]`,
            sixPrizes: `${dateHash} span[class*="v-g6-"]`,
            sevenPrizes: `${dateHash} span[class*="v-g7"]`,
            eightPrizes: `${dateHash} span[class*="v-g8"]`,
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
                const isLiveWindow = new Date().getHours() === 17 && new Date().getMinutes() >= 10 && new Date().getMinutes() <= 33;
                const intervalMs = isLiveWindow ? 2000 : 20000;

                let attempt = 0;
                const maxAttempts = 3;
                while (attempt < maxAttempts) {
                    try {
                        await page.goto(baseUrl, { waitUntil: 'domcontentloaded', timeout: 7000 });
                        await page.waitForSelector('table.kqsx-mt', { timeout: 2000 }).catch(() => {
                            console.log('Chưa thấy bảng kqsx-mt, tiếp tục cào...');
                        });
                        break;
                    } catch (error) {
                        attempt++;
                        console.warn(`Lỗi request lần ${attempt}: ${error.message}. Thử lại...`);
                        if (attempt === maxAttempts) throw error;
                        await new Promise(resolve => setTimeout(resolve, 1000));
                    }
                }

                const result = await page.evaluate(({ dateHash, selectors }) => {
                    const getPrizes = (selector) => {
                        try {
                            const elements = document.querySelectorAll(selector);
                            return Array.from(elements)
                                .map(elem => elem.getAttribute('data-id')?.trim() || '')
                                .filter(prize => prize && prize !== '...' && /^\d+$/.test(prize));
                        } catch (error) {
                            console.error(`Lỗi lấy selector ${selector}:`, error.message);
                            return [];
                        }
                    };

                    const provinces = [];
                    const provinceRow = document.querySelector('table.kqsx-mt tr.bg-pr');
                    if (provinceRow) {
                        provinceRow.querySelectorAll('th').forEach((elem, i) => {
                            if (i === 0) return;
                            const provinceName = elem.querySelector('a')?.textContent.trim();
                            if (provinceName) provinces.push(provinceName);
                        });
                    }

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

                    const resultTable = document.querySelector('table.kqsx-mt');
                    if (resultTable) {
                        resultTable.querySelectorAll('tr').forEach((row, i) => {
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
                }, { dateHash, selectors });

                if (result.provinces.length === 0) {
                    console.log('Không tìm thấy tỉnh nào trong bảng kết quả.');
                    await logPerformance(iterationStart, iteration, false, 'N/A');
                    errorCount += 1;
                    return;
                }

                result.provinces.forEach(province => {
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
                        lastPrizeDataByProvince[province].completedPrizes = {
                            specialPrize: false,
                            firstPrize: false,
                            secondPrize: false,
                            threePrizes: false,
                            fourPrizes: false,
                            fivePrizes: false,
                            sixPrizes: false,
                            sevenPrizes: false,
                            eightPrizes: false,
                        };
                        lastPrizeDataByProvince[province].stableCounts = {
                            specialPrize: 0,
                            firstPrize: 0,
                            secondPrize: 0,
                            threePrizes: 0,
                            fourPrizes: 0,
                            fivePrizes: 0,
                            sixPrizes: 0,
                            sevenPrizes: 0,
                            eightPrizes: 0,
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
                        specialPrize: result.provincesData[tentinh]?.specialPrize.length ? result.provincesData[tentinh].specialPrize : lastPrizeDataByProvince[tentinh].specialPrize,
                        firstPrize: result.provincesData[tentinh]?.firstPrize.length ? result.provincesData[tentinh].firstPrize : lastPrizeDataByProvince[tentinh].firstPrize,
                        secondPrize: result.provincesData[tentinh]?.secondPrize.length ? result.provincesData[tentinh].secondPrize : lastPrizeDataByProvince[tentinh].secondPrize,
                        threePrizes: result.provincesData[tentinh]?.threePrizes.length ? result.provincesData[tentinh].threePrizes : lastPrizeDataByProvince[tentinh].threePrizes,
                        fourPrizes: result.provincesData[tentinh]?.fourPrizes.length ? result.provincesData[tentinh].fourPrizes : lastPrizeDataByProvince[tentinh].fourPrizes,
                        fivePrizes: result.provincesData[tentinh]?.fivePrizes.length ? result.provincesData[tentinh].fivePrizes : lastPrizeDataByProvince[tentinh].fivePrizes,
                        sixPrizes: result.provincesData[tentinh]?.sixPrizes.length ? result.provincesData[tentinh].sixPrizes : lastPrizeDataByProvince[tentinh].sixPrizes,
                        sevenPrizes: result.provincesData[tentinh]?.sevenPrizes.length ? result.provincesData[tentinh].sevenPrizes : lastPrizeDataByProvince[tentinh].sevenPrizes,
                        eightPrizes: result.provincesData[tentinh]?.eightPrizes.length ? result.provincesData[tentinh].eightPrizes : lastPrizeDataByProvince[tentinh].eightPrizes,
                        station,
                        createdAt: new Date(),
                    };

                    const prizeTypes = [
                        { key: 'specialPrize', data: formattedResult.specialPrize, isArray: true, minLength: 1 },
                        { key: 'firstPrize', data: formattedResult.firstPrize, isArray: true, minLength: 1 },
                        { key: 'secondPrize', data: formattedResult.secondPrize, isArray: true, minLength: 1 },
                        { key: 'threePrizes', data: formattedResult.threePrizes, isArray: true, minLength: 2 },
                        { key: 'fourPrizes', data: formattedResult.fourPrizes, isArray: true, minLength: 7 },
                        { key: 'fivePrizes', data: formattedResult.fivePrizes, isArray: true, minLength: 1 },
                        { key: 'sixPrizes', data: formattedResult.sixPrizes, isArray: true, minLength: 3 },
                        { key: 'sevenPrizes', data: formattedResult.sevenPrizes, isArray: true, minLength: 1 },
                        { key: 'eightPrizes', data: formattedResult.eightPrizes, isArray: true, minLength: 1 },
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

                    formattedResult.specialPrize = lastPrizeDataByProvince[tentinh].specialPrize;
                    formattedResult.firstPrize = lastPrizeDataByProvince[tentinh].firstPrize;
                    formattedResult.secondPrize = lastPrizeDataByProvince[tentinh].secondPrize;
                    formattedResult.threePrizes = lastPrizeDataByProvince[tentinh].threePrizes;
                    formattedResult.fourPrizes = lastPrizeDataByProvince[tentinh].fourPrizes;
                    formattedResult.fivePrizes = lastPrizeDataByProvince[tentinh].fivePrizes;
                    formattedResult.sixPrizes = lastPrizeDataByProvince[tentinh].sixPrizes;
                    formattedResult.sevenPrizes = lastPrizeDataByProvince[tentinh].sevenPrizes;
                    formattedResult.eightPrizes = lastPrizeDataByProvince[tentinh].eightPrizes;

                    if (hasAnyData(formattedResult)) {
                        await saveToMongoDB(formattedResult);
                    } else {
                        console.log(`Dữ liệu ngày ${date} cho tỉnh ${tentinh} chưa có, tiếp tục cào...`);
                    }

                    if (!isDataComplete(formattedResult, lastPrizeDataByProvince[tentinh].completedPrizes, lastPrizeDataByProvince[tentinh].stableCounts)) {
                        allProvincesComplete = false;
                    }

                    await logPerformance(iterationStart, iteration, true, tentinh);
                }

                successCount += 1;

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
                    if (page && !page.isClosed()) await page.close();
                    if (browser) await browser.close();
                    if (release) await release();
                    if (mongooseConnected) {
                        await mongoose.connection.close();
                        mongooseConnected = false;
                        console.log('Đã đóng kết nối MongoDB');
                    }
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

        await scrapeAndSave();
        if (!isStopped) {
            intervalId = setInterval(scrapeAndSave, 3000);
        }

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
                if (page && !page.isClosed()) await page.close();
                if (browser) await browser.close();
                if (release) await release();
                if (mongooseConnected) {
                    await mongoose.connection.close();
                    mongooseConnected = false;
                    console.log('Đã đóng kết nối MongoDB');
                }
            }
        }, 17 * 60 * 1000);

    } catch (error) {
        console.error(`Lỗi khi khởi động scraper ngày ${date}:`, error.message);
        isStopped = true;
        if (page && !page.isClosed()) await page.close();
        if (browser) await browser.close();
        if (release) await release();
        if (mongooseConnected) {
            await mongoose.connection.close();
            mongooseConnected = false;
            console.log('Đã đóng kết nối MongoDB');
        }
    }
}

module.exports = { scrapeXSMT };

// Chạy thủ công nếu có tham số dòng lệnh
const [, , date, station] = process.argv;
if (date && station) {
    console.log(`Chạy thủ công cho ngày ${date} và đài ${station}`);
    scrapeXSMT(date, station);
} else {
    console.log('Nếu muốn chạy thủ công, dùng lệnh: node xsmt_scraper.js 19/04/2025 xsmt');
}

// Đóng kết nối khi dừng chương trình
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