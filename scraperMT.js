const puppeteer = require('puppeteer');
const mongoose = require('mongoose');
const redis = require('redis');
const pidusage = require('pidusage');
const { connectMongoDB, isConnected } = require('./db');
require('dotenv').config();

process.env.TZ = 'Asia/Ho_Chi_Minh';

const XSMT = require('./src/models/XS_MT.models');

// Kết nối Redis
const redisClient = redis.createClient({
    url: process.env.REDIS_URL || 'redis://localhost:6379',
});
redisClient.connect().catch(err => console.error('Lỗi kết nối Redis:', err.message));

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

// Kiểm tra dữ liệu hoàn chỉnh
function isDataComplete(result, completedPrizes, stableCounts) {
    const checkPrize = (key, data, minLength) => {
        const isValid = Array.isArray(data) && data.length === minLength && data.every(prize => prize && prize !== '...' && prize !== '****' && /^\d+$/.test(prize));
        stableCounts[key] = isValid ? (stableCounts[key] || 0) + 1 : 0;
        completedPrizes[key] = isValid && stableCounts[key] >= (key === 'specialPrize' ? 2 : 1);
        return isValid;
    };

    checkPrize('eightPrizes', result.eightPrizes || [], 1);
    checkPrize('sevenPrizes', result.sevenPrizes || [], 1);
    checkPrize('sixPrizes', result.sixPrizes || [], 3);
    checkPrize('fivePrizes', result.fivePrizes || [], 1);
    checkPrize('fourPrizes', result.fourPrizes || [], 7);
    checkPrize('threePrizes', result.threePrizes || [], 2);
    checkPrize('secondPrize', result.secondPrize || [], 1);
    checkPrize('firstPrize', result.firstPrize || [], 1);
    checkPrize('specialPrize', result.specialPrize || [], 1);

    const isComplete = result.tentinh && result.tentinh.length >= 1 &&
        Object.keys(completedPrizes).every(k => completedPrizes[k]);
    if (isComplete) console.log(`Dữ liệu hoàn thành cho tỉnh ${result.tentinh}`);
    return isComplete;
}

// Publish dữ liệu lên Redis
async function publishToRedis(changes, additionalData) {
    const { drawDate, tentinh, tinh, year, month } = additionalData;
    const today = formatDateToDDMMYYYY(new Date(drawDate));
    const redisKey = `kqxs:xsmt:${today}:${tinh}`;
    try {
        if (!redisClient.isOpen) {
            console.log(`Redis client chưa sẵn sàng, kết nối lại cho tỉnh ${tentinh}...`);
            await redisClient.connect();
        }
        console.log(`Chuẩn bị gửi ${changes.length} thay đổi tới Redis với khóa: ${redisKey}`);
        const pipeline = redisClient.multi();
        for (const { key, data } of changes) {
            pipeline.publish(`xsmt:${today}:${tinh}`, JSON.stringify({ prizeType: key, prizeData: data, drawDate: today, tentinh, tinh, year, month }));
            pipeline.hSet(redisKey, key, JSON.stringify(data));
        }
        pipeline.hSet(`${redisKey}:meta`, 'metadata', JSON.stringify({ tentinh, tinh, year, month }));
        await pipeline.exec();
        console.log(`Đã gửi ${changes.length} thay đổi qua Redis cho tỉnh ${tentinh} với khóa: ${redisKey}`);
    } catch (error) {
        console.error(`Lỗi gửi Redis cho tỉnh ${tentinh} với khóa ${redisKey}:`, error.message);
        throw error;
    }
}

// Đặt thời gian hết hạn cho Redis
async function setRedisExpiration(today, tinh) {
    const redisKey = `kqxs:xsmt:${today}:${tinh}`;
    try {
        await Promise.all([
            redisClient.expire(redisKey, 7200),
            redisClient.expire(`${redisKey}:meta`, 7200),
        ]);
        console.log(`Đã đặt expire cho ${redisKey} và metadata`);
    } catch (error) {
        console.error(`Lỗi đặt expire Redis cho tỉnh ${tinh} với khóa ${redisKey}:`, error.message);
    }
}

// Lưu dữ liệu vào MongoDB
async function saveToMongoDB(result) {
    try {
        if (!isConnected()) {
            await connectMongoDB();
        }
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

// Log chi tiết dữ liệu
function logDataDetails(province, data) {
    console.log(`Dữ liệu cho tỉnh ${province}:`, {
        eightPrizes: data.eightPrizes,
        sevenPrizes: data.sevenPrizes,
        sixPrizes: data.sixPrizes,
        fivePrizes: data.fivePrizes,
        fourPrizes: data.fourPrizes,
        threePrizes: data.threePrizes,
        secondPrize: data.secondPrize,
        firstPrize: data.firstPrize,
        specialPrize: data.specialPrize,
    });
}

// Hàm cào dữ liệu XSMT
async function scrapeXSMT(date, station, isTestMode = false) {
    let browser;
    let page;
    let intervalId;
    let isStopped = false;
    let iteration = 0;
    let successCount = 0;
    let errorCount = 0;
    const startTime = Date.now();
    const lastPrizeDataByProvince = {};

    const createNewPage = async () => {
        if (page && !page.isClosed()) return;
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
    };

    try {
        const dateParts = date.split('/');
        const dateObj = new Date(`${dateParts[2]}-${dateParts[1]}-${dateParts[0]}`);
        if (isNaN(dateObj.getTime())) {
            throw new Error('Ngày không hợp lệ: ' + date);
        }
        const formattedDate = formatDateToDDMMYYYY(dateObj).replace(/-/g, '');
        const dayOfWeekIndex = dateObj.getDay();
        const daysOfWeek = ['chu-nhat', 'thu-2', 'thu-3', 'thu-4', 'thu-5', 'thu-6', 'thu-7'];
        const dayOfWeekUrl = daysOfWeek[dayOfWeekIndex];

        await connectMongoDB();

        const isLiveWindow = new Date().getHours() === 17 && new Date().getMinutes() >= 10 && new Date().getMinutes() <= 35;
        const intervalMs = isTestMode || isLiveWindow ? 1000 : 1000;
        console.log(`intervalMs: ${intervalMs}ms (isLiveWindow: ${isLiveWindow}, isTestMode: ${isTestMode})`);

        browser = await puppeteer.launch({
            headless: true,
            args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
            executablePath: process.env.CHROMIUM_PATH || undefined,
        });
        await createNewPage();

        let baseUrl;
        if (station.toLowerCase() === 'xsmt') {
            baseUrl = dayOfWeekIndex === 0
                ? `https://xoso.com.vn/xsmt-chu-nhat-cn.html`
                : `https://xoso.com.vn/xsmt-${dayOfWeekUrl}.html`;
            console.log(`Đang cào dữ liệu từ: ${baseUrl}`);
        } else {
            throw new Error('Chỉ hỗ trợ đài xsmt trong phiên bản này');
        }

        const selectors = {
            eightPrizes: 'span.xs_prize1[id*="_prize8_item"]',
            sevenPrizes: 'span.xs_prize1[id*="_prize7_item"]',
            sixPrizes: 'span.xs_prize1[id*="_prize6_item"]',
            fivePrizes: 'span.xs_prize1[id*="_prize5_item"]',
            fourPrizes: 'span.xs_prize1[id*="_prize4_item"]',
            threePrizes: 'span.xs_prize1[id*="_prize3_item"]',
            secondPrize: 'span.xs_prize1[id*="_prize2_item"]',
            firstPrize: 'span.xs_prize1[id*="_prize1_item"]',
            specialPrize: 'span.xs_prize1[id*="_prize_Db_item"]',
        };

        const prizeLimits = {
            eightPrizes: 1,
            sevenPrizes: 1,
            sixPrizes: 3,
            fivePrizes: 1,
            fourPrizes: 7,
            threePrizes: 2,
            secondPrize: 1,
            firstPrize: 1,
            specialPrize: 1,
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
                    await page.waitForSelector(`div#mt_kqngay_${formattedDate} table.table-result.table-xsmn`, { timeout: 2000 }).catch(() => {
                        console.log('Chưa thấy bảng kết quả, tiếp tục cào...');
                    });
                } else {
                    await page.waitForSelector(`div#mt_kqngay_${formattedDate} table.table-result.table-xsmn`, { timeout: 2000 }).catch(() => {
                        console.log('Chưa thấy bảng kết quả, tiếp tục cào...');
                    });
                }

                const result = await page.evaluate(({ selectors, prizeLimits, formattedDate }) => {
                    // Định nghĩa getProvinceCode trong môi trường trình duyệt
                    const getProvinceCode = (provinceName) => {
                        // Xử lý đặc biệt cho các tỉnh
                        if (provinceName === 'Huế') return 'TTH';
                        if (provinceName === 'Kon Tum') return 'KT';
                        if (provinceName === 'Khánh Hòa') return 'KH';
                        if (provinceName === 'Quảng Ngãi') return 'QNG';
                        if (provinceName === 'Đắk Nông') return 'DNO';
                        if (provinceName === 'Đà Nẵng') return 'DNA';
                        if (provinceName === 'Quảng Nam') return 'QNA';

                        // Chuẩn hóa tên tỉnh
                        const normalized = provinceName
                            .normalize('NFD')
                            .replace(/[\u0300-\u036f]/g, '')
                            .replace(/đ/g, 'd')
                            .replace(/Đ/g, 'D');

                        const words = normalized.split(/\s+/);
                        if (words.length === 1) {
                            // Tỉnh chỉ có một từ, lấy 3 chữ cái đầu
                            return words[0].slice(0, 3).toUpperCase();
                        }
                        // Tỉnh có hai từ, lấy chữ cái đầu của từ đầu tiên và 2 chữ cái đầu của từ thứ hai
                        return (words[0][0] + words[1].slice(0, 2)).toUpperCase();
                    };

                    const getPrizeForProvince = (selector, provinceCode, limit) => {
                        try {
                            const elements = document.querySelectorAll(selector);
                            return Array.from(elements)
                                .filter(elem => elem.id.startsWith(`${provinceCode}_`))
                                .slice(0, limit)
                                .map(elem => elem.getAttribute('data-loto')?.trim() || '')
                                .filter(prize => prize && prize !== '...' && prize !== '****' && /^\d+$/.test(prize));
                        } catch (error) {
                            console.error(`Lỗi lấy selector ${selector} cho tỉnh ${provinceCode}:`, error.message);
                            return [];
                        }
                    };

                    const provinces = [];
                    const provinceRow = document.querySelectorAll(`div#mt_kqngay_${formattedDate} table.table-result.table-xsmn thead tr th h3 a`);
                    if (!provinceRow.length) {
                        return { provinces, provincesData: {}, drawDate: '' };
                    }
                    provinceRow.forEach(elem => {
                        const provinceName = elem.textContent.trim();
                        if (provinceName && !provinceName.startsWith('Tỉnh_')) {
                            provinces.push(provinceName);
                        }
                    });

                    const provincesData = {};
                    provinces.forEach(province => {
                        const provinceCode = getProvinceCode(province);
                        provincesData[province] = {
                            eightPrizes: getPrizeForProvince(selectors.eightPrizes, provinceCode, prizeLimits.eightPrizes),
                            sevenPrizes: getPrizeForProvince(selectors.sevenPrizes, provinceCode, prizeLimits.sevenPrizes),
                            sixPrizes: getPrizeForProvince(selectors.sixPrizes, provinceCode, prizeLimits.sixPrizes),
                            fivePrizes: getPrizeForProvince(selectors.fivePrizes, provinceCode, prizeLimits.fivePrizes),
                            fourPrizes: getPrizeForProvince(selectors.fourPrizes, provinceCode, prizeLimits.fourPrizes),
                            threePrizes: getPrizeForProvince(selectors.threePrizes, provinceCode, prizeLimits.threePrizes),
                            secondPrize: getPrizeForProvince(selectors.secondPrize, provinceCode, prizeLimits.secondPrize),
                            firstPrize: getPrizeForProvince(selectors.firstPrize, provinceCode, prizeLimits.firstPrize),
                            specialPrize: getPrizeForProvince(selectors.specialPrize, provinceCode, prizeLimits.specialPrize),
                        };
                    });

                    const drawDateDiv = document.querySelector(`div#mt_kqngay_${formattedDate}`);
                    const drawDate = drawDateDiv ? `${formattedDate.slice(0, 2)}/${formattedDate.slice(2, 4)}/${formattedDate.slice(4)}` : '';
                    return { provinces, provincesData, drawDate };
                }, { selectors, prizeLimits, formattedDate });

                if (result.provinces.length === 0) {
                    console.log('Không tìm thấy tỉnh nào, tiếp tục cào...');
                    await logPerformance(iterationStart, iteration, false);
                    errorCount += 1;
                    return;
                }

                const dayOfWeekIndex = dateObj.getDay();
                const dayOfWeek = ['Chủ nhật', 'Thứ 2', 'Thứ 3', 'Thứ 4', 'Thứ 5', 'Thứ 6', 'Thứ 7'][dayOfWeekIndex] || 'Thứ 2';
                let allProvincesComplete = true;

                for (const tentinh of result.provinces) {
                    if (!lastPrizeDataByProvince[tentinh]) {
                        lastPrizeDataByProvince[tentinh] = {
                            eightPrizes: Array(prizeLimits.eightPrizes).fill('...'),
                            sevenPrizes: Array(prizeLimits.sevenPrizes).fill('...'),
                            sixPrizes: Array(prizeLimits.sixPrizes).fill('...'),
                            fivePrizes: Array(prizeLimits.fivePrizes).fill('...'),
                            fourPrizes: Array(prizeLimits.fourPrizes).fill('...'),
                            threePrizes: Array(prizeLimits.threePrizes).fill('...'),
                            secondPrize: Array(prizeLimits.secondPrize).fill('...'),
                            firstPrize: Array(prizeLimits.firstPrize).fill('...'),
                            specialPrize: Array(prizeLimits.specialPrize).fill('...'),
                            completedPrizes: {
                                eightPrizes: false,
                                sevenPrizes: false,
                                sixPrizes: false,
                                fivePrizes: false,
                                fourPrizes: false,
                                threePrizes: false,
                                secondPrize: false,
                                firstPrize: false,
                                specialPrize: false,
                            },
                            stableCounts: {
                                eightPrizes: 0,
                                sevenPrizes: 0,
                                sixPrizes: 0,
                                fivePrizes: 0,
                                fourPrizes: 0,
                                threePrizes: 0,
                                secondPrize: 0,
                                firstPrize: 0,
                                specialPrize: 0,
                            },
                        };
                    }

                    const tinh = toKebabCase(tentinh);
                    const slug = `xsmt-${formatDateToDDMMYYYY(dateObj)}-${tinh}`;

                    const formattedResult = {
                        drawDate: dateObj,
                        slug,
                        year: dateObj.getFullYear(),
                        month: dateObj.getMonth() + 1,
                        dayOfWeek,
                        tentinh,
                        tinh,
                        eightPrizes: result.provincesData[tentinh]?.eightPrizes?.length ? result.provincesData[tentinh].eightPrizes : lastPrizeDataByProvince[tentinh].eightPrizes,
                        sevenPrizes: result.provincesData[tentinh]?.sevenPrizes?.length ? result.provincesData[tentinh].sevenPrizes : lastPrizeDataByProvince[tentinh].sevenPrizes,
                        sixPrizes: result.provincesData[tentinh]?.sixPrizes?.length ? result.provincesData[tentinh].sixPrizes : lastPrizeDataByProvince[tentinh].sixPrizes,
                        fivePrizes: result.provincesData[tentinh]?.fivePrizes?.length ? result.provincesData[tentinh].fivePrizes : lastPrizeDataByProvince[tentinh].fivePrizes,
                        fourPrizes: result.provincesData[tentinh]?.fourPrizes?.length ? result.provincesData[tentinh].fourPrizes : lastPrizeDataByProvince[tentinh].fourPrizes,
                        threePrizes: result.provincesData[tentinh]?.threePrizes?.length ? result.provincesData[tentinh].threePrizes : lastPrizeDataByProvince[tentinh].threePrizes,
                        secondPrize: result.provincesData[tentinh]?.secondPrize?.length ? result.provincesData[tentinh].secondPrize : lastPrizeDataByProvince[tentinh].secondPrize,
                        firstPrize: result.provincesData[tentinh]?.firstPrize?.length ? result.provincesData[tentinh].firstPrize : lastPrizeDataByProvince[tentinh].firstPrize,
                        specialPrize: result.provincesData[tentinh]?.specialPrize?.length ? result.provincesData[tentinh].specialPrize : lastPrizeDataByProvince[tentinh].specialPrize,
                        station,
                        createdAt: new Date(),
                    };

                    logDataDetails(tentinh, formattedResult);

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
                            for (let index = 0; index < Math.min(data.length, minLength); index++) {
                                const prize = data[index];
                                if (prize && prize !== '...' && prize !== '****' && /^\d+$/.test(prize) && prize !== lastPrizeDataByProvince[tentinh][key][index]) {
                                    changes.push({ key: `${key}_${index}`, data: prize });
                                    lastPrizeDataByProvince[tentinh][key][index] = prize;
                                }
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

                    if (isDataComplete(formattedResult, lastPrizeDataByProvince[tentinh].completedPrizes, lastPrizeDataByProvince[tentinh].stableCounts)) {
                        console.log(`Dữ liệu ngày ${date} cho tỉnh ${tentinh} đã đầy đủ.`);
                        await saveToMongoDB(formattedResult);
                        await setRedisExpiration(formatDateToDDMMYYYY(dateObj), tinh);
                    } else {
                        allProvincesComplete = false;
                    }
                }

                await logPerformance(iterationStart, iteration, true);
                successCount += 1;

                if (allProvincesComplete) {
                    console.log(`Dữ liệu ngày ${date} cho tất cả tỉnh đã đầy đủ, dừng cào.`);
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
                    return;
                }
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
        if (page && !page.isClosed()) await page.close();
        if (browser) await browser.close();
    }
}

module.exports = { scrapeXSMT };

const [, , date, station, testMode] = process.argv;
if (date && station) {
    const isTestMode = testMode === 'test';
    console.log(`Chạy thủ công cho ngày ${date} và đài ${station}${isTestMode ? ' (chế độ thử nghiệm)' : ''}`);
    scrapeXSMT(date, station, isTestMode);
} else {
    console.log('Chạy thủ công: node xsmt_scraper.js 18/06/2025 xsmt [test]');
}

process.on('SIGINT', async () => {
    await redisClient.quit();
    console.log('Đã đóng kết nối Redis MIỀN TRUNG');
    process.exit(0);
});