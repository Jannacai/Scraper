const puppeteer = require('puppeteer');
const mongoose = require('mongoose');
const redis = require('redis');
const pidusage = require('pidusage');
require('dotenv').config();

process.env.TZ = 'Asia/Ho_Chi_Minh';

const XSMB = require('./src/models/XS_MB.models');

let mongooseConnected = false;
async function connectMongoDB() {
    if (mongooseConnected || mongoose.connection.readyState === 1) {
        console.log('MongoDB already connected');
        return;
    }
    try {
        await mongoose.connect(process.env.MONGODB_URI || 'mongodb://localhost:27017/xsmb', {
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

    checkPrize('specialPrize', result.specialPrize || [], 1);
    checkPrize('firstPrize', result.firstPrize || [], 1);
    checkPrize('secondPrize', result.secondPrize || [], 2);
    checkPrize('threePrizes', result.threePrizes || [], 6);
    checkPrize('fourPrizes', result.fourPrizes || [], 4);
    checkPrize('fivePrizes', result.fivePrizes || [], 6);
    checkPrize('sixPrizes', result.sixPrizes || [], 3);
    checkPrize('sevenPrizes', result.sevenPrizes || [], 4);

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
        for (const { key, data, status } of changes) {
            const message = {
                prizeType: key,
                prizeData: data,
                status: status || '...',
                drawDate: today,
                tentinh,
                tinh,
                year,
                month
            };
            pipeline.publish(`xsmb:${today}`, JSON.stringify(message));
            pipeline.hSet(`kqxs:${today}`, key, JSON.stringify({ data, status: status || '...' }));
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
        const existingResult = await XSMB.findOne({ drawDate: result.drawDate, station: result.station }).lean();
        if (existingResult) {
            const existingData = {
                maDB: existingResult.maDB,
                specialPrize: existingResult.specialPrize,
                firstPrize: existingResult.firstPrize,
                secondPrize: existingResult.secondPrize,
                threePrizes: existingResult.threePrizes,
                fourPrizes: existingResult.fourPrizes,
                fivePrizes: existingResult.fivePrizes,
                sixPrizes: existingResult.sixPrizes,
                sevenPrizes: existingResult.sevenPrizes,
            };
            const newData = {
                maDB: result.maDB,
                specialPrize: result.specialPrize,
                firstPrize: result.firstPrize,
                secondPrize: result.secondPrize,
                threePrizes: result.threePrizes,
                fourPrizes: result.fourPrizes,
                fivePrizes: result.fivePrizes,
                sixPrizes: result.sixPrizes,
                sevenPrizes: result.sevenPrizes,
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
        maDB: '...',
        specialPrize: ['...'],
        firstPrize: ['...'],
        secondPrize: ['...', '...'],
        threePrizes: ['...', '...', '...', '...', '...', '...'],
        fourPrizes: ['...', '...', '...', '...'],
        fivePrizes: ['...', '...', '...', '...', '...', '...'],
        sixPrizes: ['...', '...', '...'],
        sevenPrizes: ['...', '...', '...', '...'],
    };
    const completedPrizes = {
        maDB: false,
        specialPrize: false,
        firstPrize: false,
        secondPrize: false,
        threePrizes: false,
        fourPrizes: false,
        fivePrizes: false,
        sixPrizes: false,
        sevenPrizes: false,
    };
    const stableCounts = {
        maDB: 0,
        specialPrize: 0,
        firstPrize: 0,
        secondPrize: 0,
        threePrizes: 0,
        fourPrizes: 0,
        fivePrizes: 0,
        sixPrizes: 0,
        sevenPrizes: 0,
    };
    let formattedResult = {
        drawDate: null,
        slug: '',
        year: 0,
        month: 0,
        dayOfWeek: '',
        maDB: '...',
        tentinh: '',
        tinh: '',
        specialPrize: ['...'],
        firstPrize: ['...'],
        secondPrize: ['...', '...'],
        threePrizes: ['...', '...', '...', '...', '...', '...'],
        fourPrizes: ['...', '...', '...', '...'],
        fivePrizes: ['...', '...', '...', '...', '...', '...'],
        sixPrizes: ['...', '...', '...'],
        sevenPrizes: ['...', '...', '...', '...'],
        station: '',
        createdAt: new Date(),
    };

    try {
        const dateParts = date.split('/');
        const dateObj = new Date(`${dateParts[2]}-${dateParts[1]}-${dateParts[0]}`);
        if (isNaN(dateObj.getTime())) {
            throw new Error('Ngày không hợp lệ: ' + date);
        }
        const formattedDate = date.replace(/\//g, '-');

        const isLiveWindow = new Date().getHours() === 18 && new Date().getMinutes() >= 15 && new Date().getMinutes() <= 35;
        const intervalMs = isTestMode || isLiveWindow ? 2000 : 10000;
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
            maDB: `${dateHash} span[class*="v-madb"]:first-child`,
            specialPrize: `${dateHash} span[class*="v-gdb"]`,
            firstPrize: `${dateHash} span[class*="v-g1"]`,
            secondPrize: `${dateHash} span[class*="v-g2-"]`,
            threePrizes: `${dateHash} span[class*="v-g3-"]`,
            fourPrizes: `${dateHash} span[class*="v-g4-"]`,
            fivePrizes: `${dateHash} span[class*="v-g5-"]`,
            sixPrizes: `${dateHash} span[class*="v-g6-"]`,
            sevenPrizes: `${dateHash} span[class*="v-g7-"]`,
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

                const prizeOrder = Object.keys(completedPrizes).filter(key => !completedPrizes[key]);
                const result = await page.evaluate(({ dateHash, selectors, prizeOrder }) => {
                    const getPrizes = (selector, isMaDB = false) => {
                        try {
                            const elements = document.querySelectorAll(selector);
                            return Array.from(elements).map(elem => {
                                const outputDiv = elem.querySelector('div.output');
                                if (outputDiv && !isMaDB) {
                                    return { value: '...', status: 'animating' };
                                }
                                const text = elem.textContent.trim();
                                return {
                                    value: text && /^\d+$/.test(text) ? text : '...',
                                    status: isMaDB ? '...' : (text && /^\d+$/.test(text) ? 'complete' : '...')
                                };
                            });
                        } catch (error) {
                            console.error(`Lỗi lấy selector ${selector}:`, error.message);
                            return [{ value: '...', status: '...' }];
                        }
                    };

                    const result = { drawDate: document.querySelector('.ngay_quay')?.textContent.trim() || '' };
                    for (const prizeType of prizeOrder) {
                        if (prizeType === 'maDB') {
                            const maDBElement = document.querySelector(selectors.maDB);
                            result.maDB = maDBElement ? { value: maDBElement.textContent.trim() || '...', status: '...' } : { value: '...', status: '...' };
                        } else {
                            result[prizeType] = getPrizes(selectors[prizeType], prizeType === 'maDB');
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

                // Cập nhật formattedResult với dữ liệu mới
                formattedResult = {
                    drawDate: dateObj,
                    slug,
                    year: dateObj.getFullYear(),
                    month: dateObj.getMonth() + 1,
                    dayOfWeek,
                    maDB: result.maDB.value || lastPrizeData.maDB,
                    tentinh,
                    tinh,
                    specialPrize: Array.isArray(result.specialPrize) && result.specialPrize.length ? result.specialPrize.map(item => item.value) : lastPrizeData.specialPrize,
                    firstPrize: Array.isArray(result.firstPrize) && result.firstPrize.length ? result.firstPrize.map(item => item.value) : lastPrizeData.firstPrize,
                    secondPrize: Array.isArray(result.secondPrize) && result.secondPrize.length ? result.secondPrize.map(item => item.value).slice(0, 2) : lastPrizeData.secondPrize,
                    threePrizes: Array.isArray(result.threePrizes) && result.threePrizes.length ? result.threePrizes.map(item => item.value).slice(0, 6) : lastPrizeData.threePrizes,
                    fourPrizes: Array.isArray(result.fourPrizes) && result.fourPrizes.length ? result.fourPrizes.map(item => item.value).slice(0, 4) : lastPrizeData.fourPrizes,
                    fivePrizes: Array.isArray(result.fivePrizes) && result.fivePrizes.length ? result.fivePrizes.map(item => item.value).slice(0, 6) : lastPrizeData.fivePrizes,
                    sixPrizes: Array.isArray(result.sixPrizes) && result.sixPrizes.length ? result.sixPrizes.map(item => item.value).slice(0, 3) : lastPrizeData.sixPrizes,
                    sevenPrizes: Array.isArray(result.sevenPrizes) && result.sevenPrizes.length ? result.sevenPrizes.map(item => item.value).slice(0, 4) : lastPrizeData.sevenPrizes,
                    station,
                    createdAt: new Date(),
                };

                const prizeTypes = [
                    { key: 'maDB', data: formattedResult.maDB, status: undefined, isArray: false, minLength: 1 },
                    { key: 'specialPrize_0', data: formattedResult.specialPrize[0], status: result.specialPrize[0]?.status || '...', isArray: false, minLength: 1 },
                    { key: 'firstPrize_0', data: formattedResult.firstPrize[0], status: result.firstPrize[0]?.status || '...', isArray: false, minLength: 1 },
                    { key: 'secondPrize_0', data: formattedResult.secondPrize[0], status: result.secondPrize[0]?.status || '...', isArray: false, minLength: 1 },
                    { key: 'secondPrize_1', data: formattedResult.secondPrize[1], status: result.secondPrize[1]?.status || '...', isArray: false, minLength: 1 },
                    { key: 'threePrizes_0', data: formattedResult.threePrizes[0], status: result.threePrizes[0]?.status || '...', isArray: false, minLength: 1 },
                    { key: 'threePrizes_1', data: formattedResult.threePrizes[1], status: result.threePrizes[1]?.status || '...', isArray: false, minLength: 1 },
                    { key: 'threePrizes_2', data: formattedResult.threePrizes[2], status: result.threePrizes[2]?.status || '...', isArray: false, minLength: 1 },
                    { key: 'threePrizes_3', data: formattedResult.threePrizes[3], status: result.threePrizes[3]?.status || '...', isArray: false, minLength: 1 },
                    { key: 'threePrizes_4', data: formattedResult.threePrizes[4], status: result.threePrizes[4]?.status || '...', isArray: false, minLength: 1 },
                    { key: 'threePrizes_5', data: formattedResult.threePrizes[5], status: result.threePrizes[5]?.status || '...', isArray: false, minLength: 1 },
                    { key: 'fourPrizes_0', data: formattedResult.fourPrizes[0], status: result.fourPrizes[0]?.status || '...', isArray: false, minLength: 1 },
                    { key: 'fourPrizes_1', data: formattedResult.fourPrizes[1], status: result.fourPrizes[1]?.status || '...', isArray: false, minLength: 1 },
                    { key: 'fourPrizes_2', data: formattedResult.fourPrizes[2], status: result.fourPrizes[2]?.status || '...', isArray: false, minLength: 1 },
                    { key: 'fourPrizes_3', data: formattedResult.fourPrizes[3], status: result.fourPrizes[3]?.status || '...', isArray: false, minLength: 1 },
                    { key: 'fivePrizes_0', data: formattedResult.fivePrizes[0], status: result.fivePrizes[0]?.status || '...', isArray: false, minLength: 1 },
                    { key: 'fivePrizes_1', data: formattedResult.fivePrizes[1], status: result.fivePrizes[1]?.status || '...', isArray: false, minLength: 1 },
                    { key: 'fivePrizes_2', data: formattedResult.fivePrizes[2], status: result.fivePrizes[2]?.status || '...', isArray: false, minLength: 1 },
                    { key: 'fivePrizes_3', data: formattedResult.fivePrizes[3], status: result.fivePrizes[3]?.status || '...', isArray: false, minLength: 1 },
                    { key: 'fivePrizes_4', data: formattedResult.fivePrizes[4], status: result.fivePrizes[4]?.status || '...', isArray: false, minLength: 1 },
                    { key: 'fivePrizes_5', data: formattedResult.fivePrizes[5], status: result.fivePrizes[5]?.status || '...', isArray: false, minLength: 1 },
                    { key: 'sixPrizes_0', data: formattedResult.sixPrizes[0], status: result.sixPrizes[0]?.status || '...', isArray: false, minLength: 1 },
                    { key: 'sixPrizes_1', data: formattedResult.sixPrizes[1], status: result.sixPrizes[1]?.status || '...', isArray: false, minLength: 1 },
                    { key: 'sixPrizes_2', data: formattedResult.sixPrizes[2], status: result.sixPrizes[2]?.status || '...', isArray: false, minLength: 1 },
                    { key: 'sevenPrizes_0', data: formattedResult.sevenPrizes[0], status: result.sevenPrizes[0]?.status || '...', isArray: false, minLength: 1 },
                    { key: 'sevenPrizes_1', data: formattedResult.sevenPrizes[1], status: result.sevenPrizes[1]?.status || '...', isArray: false, minLength: 1 },
                    { key: 'sevenPrizes_2', data: formattedResult.sevenPrizes[2], status: result.sevenPrizes[2]?.status || '...', isArray: false, minLength: 1 },
                    { key: 'sevenPrizes_3', data: formattedResult.sevenPrizes[3], status: result.sevenPrizes[3]?.status || '...', isArray: false, minLength: 1 },
                ];

                const changes = [];
                for (const { key, data, status } of prizeTypes) {
                    const keyBase = key.split('_')[0];
                    const index = parseInt(key.split('_')[1] || '0');
                    if (key === 'maDB' && data && data !== '...' && data !== '' && data !== lastPrizeData[key]) {
                        changes.push({ key, data }); // Không gửi status cho maDB
                        lastPrizeData[key] = data;
                    } else if (data && data !== '...' && /^\d+$/.test(data) && data !== lastPrizeData[keyBase][index]) {
                        changes.push({ key, data, status }); // Gửi data và status cho giải
                        lastPrizeData[keyBase][index] = data;
                    } else if (status === 'animating' && lastPrizeData[keyBase][index] !== '...') {
                        changes.push({ key, data: '...', status: 'animating' }); // Gửi animating
                        lastPrizeData[keyBase][index] = '...';
                    }
                }

                if (changes.length) {
                    await publishToRedis(changes, formattedResult);
                }

                formattedResult.maDB = lastPrizeData.maDB;
                formattedResult.specialPrize = lastPrizeData.specialPrize;
                formattedResult.firstPrize = lastPrizeData.firstPrize;
                formattedResult.secondPrize = lastPrizeData.secondPrize;
                formattedResult.threePrizes = lastPrizeData.threePrizes;
                formattedResult.fourPrizes = lastPrizeData.fourPrizes;
                formattedResult.fivePrizes = lastPrizeData.fivePrizes;
                formattedResult.sixPrizes = lastPrizeData.sixPrizes;
                formattedResult.sevenPrizes = lastPrizeData.sevenPrizes;

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
                    if (mongooseConnected) {
                        await mongoose.connection.close();
                        mongooseConnected = false;
                        console.log('Đã đóng kết nối MongoDB');
                    }
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
                console.log(`Dữ liệu ngày ${date} cho ${station} dừng sau 17 phút.`);
                await saveToMongoDB(formattedResult); // Sử dụng formattedResult đã được khởi tạo
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
        await setRedisExpiration(formatDateToDDMMYYYY(dateObj || new Date()));
        if (page && !page.isClosed()) await page.close();
        if (browser) await browser.close();
        if (mongooseConnected) {
            await mongoose.connection.close();
            mongooseConnected = false;
            console.log('Đã đóng kết nối MongoDB');
        }
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
    if (mongooseConnected) {
        await mongoose.connection.close();
        mongooseConnected = false;
        console.log('Đã đóng kết nối MongoDB');
    }
    await redisClient.quit();
    console.log('Đã đóng kết nối Redis');
    process.exit(0);
});