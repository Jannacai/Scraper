const puppeteer = require('puppeteer');
const mongoose = require('mongoose');
const { lock } = require('proper-lockfile');
const fs = require('fs');
const path = require('path');
const redis = require('redis');
const pidusage = require('pidusage');
require('dotenv').config();

// Cố định múi giờ GMT+7
process.env.TZ = 'Asia/Ho_Chi_Minh';

const XSMB = require('./src/models/XS_MB.models');

let mongooseConnected = false;
async function connectMongoDB() {
    // console.log('MongoDB readyState:', mongoose.connection.readyState, 'Connection status: Attempting...');
    if (mongooseConnected || mongoose.connection.readyState === 1) {
        await mongoose.connection.asPromise().catch(() => { });
        // console.log('MongoDB already connected');
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

const lockFilePath = path.resolve(__dirname, 'scraper.lock');

const ensureLockFile = () => {
    try {
        if (fs.existsSync(lockFilePath)) {
            // console.log(`Lock file ${lockFilePath} đã tồn tại, thử xóa...`);
            fs.unlinkSync(lockFilePath);
        }
        fs.writeFileSync(lockFilePath, '');
        // console.log(`Tạo file ${lockFilePath}`);
    } catch (error) {
        console.error(`Lỗi khi xử lý file ${lockFilePath}:`, error.message);
        throw error;
    }
};

function formatDateToDDMMYYYY(date) {
    const day = String(date.getDate()).padStart(2, '0');
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const year = date.getFullYear();
    return `${day}-${month}-${year}`;
}

function isDataComplete(result, completedPrizes, stableCounts, startTime) {
    // console.log('Raw maDB:', result.maDB);
    const isValidMaDB = result.maDB && typeof result.maDB === 'string' && result.maDB.trim() !== '' && result.maDB.trim() !== '...';
    if (!isValidMaDB) {
        // console.log('maDB không hợp lệ:', {
        //     value: result.maDB,
        //     isString: typeof result.maDB === 'string',
        //     trimmed: result.maDB && result.maDB.trim(),
        // });
    }

    const checkPrize = (key, data, minLength) => {
        const isValid = Array.isArray(data) && data.length >= minLength && data.every(prize => prize && prize !== '...' && prize !== '****' && /^\d+$/.test(prize));
        if (isValid) {
            stableCounts[key] = (stableCounts[key] || 0) + 1;
            if (key === 'specialPrize') {
                // console.log(`Giải đặc biệt hợp lệ, stableCounts: ${stableCounts[key]}`);
                if (stableCounts[key] === 1) {
                    // console.log(`specialPrize hợp lệ đầu tiên tại: ${(Date.now() - startTime) / 1000} giây`);
                }
                if (stableCounts[key] >= 3) {
                    completedPrizes[key] = true;
                    // console.log('Giải đặc biệt hoàn thành');
                } else if (
                    Object.keys(completedPrizes).every(k => k === 'specialPrize' || completedPrizes[k]) &&
                    Date.now() - startTime > 5 * 60 * 1000
                ) {
                    if (!completedPrizes[key]) {
                        // console.log(`Tất cả giải khác hoàn thành tại: ${(Date.now() - startTime) / 1000} giây, bắt đầu chờ specialPrize timeout`);
                    }
                    console.warn('Timeout chờ giải đặc biệt, sử dụng giá trị cuối cùng:', data);
                    completedPrizes[key] = true;
                }
            } else {
                if (stableCounts[key] >= 1) {
                    completedPrizes[key] = true;
                }
            }
        } else {
            stableCounts[key] = 0;
            // console.log(`Giải ${key} không hợp lệ:`, data, `Yêu cầu minLength: ${minLength}`);
        }
        return isValid;
    };

    stableCounts.maDB = stableCounts.maDB || 0;
    if (isValidMaDB) stableCounts.maDB += 1;
    completedPrizes.maDB = isValidMaDB && (stableCounts.maDB >= 1);

    checkPrize('specialPrize', result.specialPrize, 1);
    checkPrize('firstPrize', result.firstPrize, 1);
    checkPrize('secondPrize', result.secondPrize, 2);
    checkPrize('threePrizes', result.threePrizes, 6);
    checkPrize('fourPrizes', result.fourPrizes, 4);
    checkPrize('fivePrizes', result.fivePrizes, 6);
    checkPrize('sixPrizes', result.sixPrizes, 3);
    checkPrize('sevenPrizes', result.sevenPrizes, 4);

    if (Object.keys(completedPrizes).every(k => k === 'specialPrize' || completedPrizes[k]) && !completedPrizes.specialPrize) {
        // console.log(`Tất cả giải khác hoàn thành tại: ${(Date.now() - startTime) / 1000} giây`);
    }

    const isComplete = (
        completedPrizes.maDB &&
        result.tentinh && result.tentinh.length >= 1 &&
        completedPrizes.specialPrize &&
        completedPrizes.firstPrize &&
        completedPrizes.secondPrize &&
        completedPrizes.threePrizes &&
        completedPrizes.fourPrizes &&
        completedPrizes.fivePrizes &&
        completedPrizes.sixPrizes &&
        completedPrizes.sevenPrizes
    );

    if (!isComplete) {
        // console.log('isDataComplete thất bại:', {
        //     maDB: completedPrizes.maDB,
        //     tentinh: result.tentinh,
        //     specialPrize: completedPrizes.specialPrize,
        //     firstPrize: completedPrizes.firstPrize,
        //     secondPrize: completedPrizes.secondPrize,
        //     threePrizes: completedPrizes.threePrizes,
        //     fourPrizes: completedPrizes.fourPrizes,
        //     fivePrizes: completedPrizes.fivePrizes,
        //     sixPrizes: completedPrizes.sixPrizes,
        //     sevenPrizes: completedPrizes.sevenPrizes,
        //     stableCounts
        // });
    }

    return isComplete;
}

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
    // console.log(`Gửi Redis: ${prizeType}`, prizeData, `Kênh: xsmb:${today}`);

    try {
        if (!redisClient.isOpen) {
            // console.log('Redis client chưa sẵn sàng, kết nối lại...');
            await redisClient.connect();
        }
        await redisClient.publish(`xsmb:${today}`, message);
        await redisClient.hSet(`kqxs:${today}`, prizeType, JSON.stringify(prizeData)).catch(err => console.error(`Lỗi hSet ${prizeType}:`, err.message));
        await redisClient.hSet(`kqxs:${today}:meta`, 'metadata', JSON.stringify({ tentinh, tinh, year, month })).catch(err => console.error('Lỗi hSet metadata:', err.message));
        // console.log(`Đã gửi ${prizeType} và metadata qua Redis cho ngày ${today}`);
    } catch (error) {
        console.error(`Lỗi gửi Redis (${prizeType}):`, error.message);
        throw error;
    }
}

async function setRedisExpiration(today) {
    try {
        await Promise.all([
            redisClient.expire(`kqxs:${today}`, 7200),
            redisClient.expire(`kqxs:${today}:meta`, 7200),
        ]);
        // console.log(`Đã đặt expire cho kqxs:${today} và metadata`);
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
                // console.log(`Cập nhật kết quả ngày ${result.drawDate.toISOString().split('T')[0]} cho ${result.station}`);
            }
        } else {
            await XSMB.create(result);
            // console.log(`Lưu kết quả mới ngày ${result.drawDate.toISOString().split('T')[0]} cho ${result.station}`);
        }
    } catch (error) {
        console.error(`Lỗi khi lưu dữ liệu ngày ${result.drawDate.toISOString().split('T')[0]}:`, error.message);
    }
}

async function logPerformance(startTime, iteration, success) {
    const stats = await pidusage(process.pid);
    const duration = (Date.now() - startTime) / 1000;
    console.log(`Lần cào ${iteration} (${success ? 'Thành công' : 'Thất bại'}):`, {
        duration: `${duration.toFixed(2)}s`,
        cpu: `${stats.cpu.toFixed(2)}%`,
        memory: `${(stats.memory / 1024 / 1024).toFixed(2)}MB`,
    });
}

async function scrapeXSMB(date, station, isTestMode = false) {
    let browser;
    let page;
    let intervalId;
    let release;
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
    const prevPrizeData = JSON.parse(JSON.stringify(lastPrizeData));
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

    try {
        const dateParts = date.split('/');
        const dateObj = new Date(`${dateParts[2]}-${dateParts[1]}-${dateParts[0]}`);
        if (isNaN(dateObj.getTime())) {
            throw new Error('Ngày không hợp lệ: ' + date);
        }
        const formattedDate = date.replace(/\//g, '-');

        // Kiểm tra múi giờ
        const now = new Date();
        const timezoneOffset = now.getTimezoneOffset();
        // console.log(`Múi giờ hệ thống: GMT${timezoneOffset >= 0 ? '-' : '+'}${Math.abs(timezoneOffset / 60)} (offset: ${timezoneOffset} phút)`);
        if (timezoneOffset !== -420) {
            console.warn('Múi giờ không phải GMT+7, đã cố định bằng TZ=Asia/Ho_Chi_Minh');
        }

        // Xác định intervalMs
        const isLiveWindow = now.getHours() === 18 && now.getMinutes() >= 15 && now.getMinutes() <= 35;
        const intervalMs = isTestMode || isLiveWindow ? 2000 : 30000;
        // console.log(`intervalMs được đặt thành: ${intervalMs}ms (isLiveWindow: ${isLiveWindow}, isTestMode: ${isTestMode})`);
        if (!isLiveWindow && !isTestMode) {
            console.warn('Chạy ngoài khung trực tiếp (18:15-18:35) và không ở chế độ thử nghiệm, intervalMs = 30000ms. Dùng "node scraper.js <ngày> xsmb test" để mô phỏng khung trực tiếp.');
        }

        await connectMongoDB();
        ensureLockFile();
        release = await lock(lockFilePath, { retries: 3, stale: 30000 });

        browser = await puppeteer.launch({
            headless: 'new',
            args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
            executablePath: process.env.CHROMIUM_PATH || undefined,
        });
        page = await browser.newPage();
        await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124');

        let baseUrl, dateHash;
        if (station.toLowerCase() === 'xsmb') {
            baseUrl = `https://xosovn.com/xsmb-${formattedDate}`;
            dateHash = `#kqngay_${formattedDate.split('-').join('')}`;
            // console.log(`Đang cào dữ liệu từ: ${baseUrl}`);
        } else {
            throw new Error('Chỉ hỗ trợ đài xsmb trong phiên bản này');
        }

        const scrapeAndSave = async () => {
            if (isStopped || (page && page.isClosed())) {
                // console.log(`Scraper đã dừng hoặc page đã đóng, bỏ qua lần cào ${iteration + 1}`);
                clearInterval(intervalId);
                return;
            }

            iteration += 1;
            const iterationStart = Date.now();
            // console.log(`Bắt đầu lần cào ${iteration} tại: ${(iterationStart - startTime) / 1000} giây`);
            try {
                let attempt = 0;
                const maxAttempts = 5;
                while (attempt < maxAttempts) {
                    try {
                        if (iteration === 1) {
                            await page.goto(baseUrl, { waitUntil: 'domcontentloaded', timeout: 7000 });
                        } else {
                            await page.reload({ waitUntil: 'domcontentloaded', timeout: 7000 });
                        }
                        await page.waitForSelector(`${dateHash} span[class*="v-madb"]`, { timeout: 1000 }).catch(() => {
                            // console.log('Chưa thấy maDB, tiếp tục cào...');
                        });
                        break;
                    } catch (error) {
                        attempt++;
                        console.warn(`Lỗi request lần ${attempt}: ${error.message}. Thử lại...`);
                        if (attempt === maxAttempts) throw error;
                        await new Promise(resolve => setTimeout(resolve, 1000 * attempt));
                    }
                    await new Promise(resolve => setTimeout(resolve, Math.random() * 400 + 100));
                }

                const selectors = {
                    maDB: `${dateHash} span[class*="v-madb"], ${dateHash} [class*="madb"], ${dateHash} .madb`,
                    specialPrize: `${dateHash} span[class*="v-gdb"]`,
                    firstPrize: `${dateHash} span[class*="v-g1"]`,
                    secondPrize: `${dateHash} span[class*="v-g2-"]`,
                    threePrizes: `${dateHash} span[class*="v-g3-"]`,
                    fourPrizes: `${dateHash} span[class*="v-g4-"]`,
                    fivePrizes: `${dateHash} span[class*="v-g5-"]`,
                    sixPrizes: `${dateHash} span[class*="v-g6-"]`,
                    sevenPrizes: `${dateHash} span[class*="v-g7-"]`,
                };

                const allOtherPrizesCompleted = Object.keys(completedPrizes).every(key => key === 'specialPrize' || completedPrizes[key]);
                const prizeOrder = allOtherPrizesCompleted ? ['specialPrize'] : [
                    'firstPrize',
                    'secondPrize',
                    'threePrizes',
                    'fourPrizes',
                    'fivePrizes',
                    'sixPrizes',
                    'sevenPrizes',
                    'maDB',
                    'specialPrize',
                ];
                const result = await page.evaluate(({ dateHash, selectors, completedPrizes, prizeOrder }) => {
                    const getPrizes = (selector) => {
                        try {
                            const elements = document.querySelectorAll(selector);
                            return Array.from(elements).map(elem => elem.textContent.trim()).filter(prize => prize && prize !== '...' && prize !== '****');
                        } catch (error) {
                            console.error(`Lỗi khi lấy dữ liệu cho selector ${selector}:`, error.message);
                            return [];
                        }
                    };

                    try {
                        const maDBElements = document.querySelectorAll(selectors.maDB);
                        if (maDBElements.length > 1) {
                            console.warn(`Cảnh báo: Tìm thấy ${maDBElements.length} element khớp selector maDB, lấy element đầu tiên`);
                        }
                        const maDB = maDBElements.length > 0 ? maDBElements[0].textContent.trim() : '...';

                        const result = {
                            drawDate: document.querySelector('.ngay_quay')?.textContent.trim() || '',
                            maDB,
                            specialPrize: [],
                            firstPrize: [],
                            secondPrize: [],
                            threePrizes: [],
                            fourPrizes: [],
                            fivePrizes: [],
                            sixPrizes: [],
                            sevenPrizes: [],
                        };

                        for (const prizeType of prizeOrder) {
                            if (prizeType !== 'maDB' && !completedPrizes[prizeType]) {
                                result[prizeType] = getPrizes(selectors[prizeType]);
                            }
                        }

                        return result;
                    } catch (error) {
                        console.error('Lỗi trong page.evaluate:', error.message);
                        return {
                            drawDate: '',
                            maDB: '...',
                            specialPrize: [],
                            firstPrize: [],
                            secondPrize: [],
                            threePrizes: [],
                            fourPrizes: [],
                            fivePrizes: [],
                            sixPrizes: [],
                            sevenPrizes: [],
                        };
                    }
                }, { dateHash, selectors, completedPrizes, prizeOrder });

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
                    default:
                        console.error('Không xác định được ngày trong tuần, gán mặc định');
                        tinh = 'ha-noi'; tentinh = 'Hà Nội';
                }

                const slug = `${station}-${formattedDate}`;
                const dayOfWeek = ['Chủ nhật', 'Thứ 2', 'Thứ 3', 'Thứ 4', 'Thứ 5', 'Thứ 6', 'Thứ 7'][dayOfWeekIndex] || 'Thứ 2';

                const formattedResult = {
                    drawDate: dateObj,
                    slug,
                    year: dateObj.getFullYear(),
                    month: dateObj.getMonth() + 1,
                    dayOfWeek,
                    maDB: result.maDB !== '...' ? result.maDB : lastPrizeData.maDB,
                    tentinh,
                    tinh,
                    specialPrize: result.specialPrize.length ? result.specialPrize : lastPrizeData.specialPrize,
                    firstPrize: result.firstPrize.length ? result.firstPrize : lastPrizeData.firstPrize,
                    secondPrize: result.secondPrize.length ? result.secondPrize : lastPrizeData.secondPrize,
                    threePrizes: result.threePrizes.length ? result.threePrizes : lastPrizeData.threePrizes,
                    fourPrizes: result.fourPrizes.length ? result.fourPrizes : lastPrizeData.fourPrizes,
                    fivePrizes: result.fivePrizes.length ? result.fivePrizes : lastPrizeData.fivePrizes,
                    sixPrizes: result.sixPrizes.length ? result.sixPrizes : lastPrizeData.sixPrizes,
                    sevenPrizes: result.sevenPrizes.length ? result.sevenPrizes : lastPrizeData.sevenPrizes,
                    station,
                    createdAt: new Date(),
                };

                const prizeTypes = [
                    { key: 'maDB', data: formattedResult.maDB, isArray: false, minLength: 1 },
                    { key: 'specialPrize', data: formattedResult.specialPrize, isArray: true, minLength: 1 },
                    { key: 'firstPrize', data: formattedResult.firstPrize, isArray: true, minLength: 1 },
                    { key: 'secondPrize', data: formattedResult.secondPrize, isArray: true, minLength: 2 },
                    { key: 'threePrizes', data: formattedResult.threePrizes, isArray: true, minLength: 6 },
                    { key: 'fourPrizes', data: formattedResult.fourPrizes, isArray: true, minLength: 4 },
                    { key: 'fivePrizes', data: formattedResult.fivePrizes, isArray: true, minLength: 6 },
                    { key: 'sixPrizes', data: formattedResult.sixPrizes, isArray: true, minLength: 3 },
                    { key: 'sevenPrizes', data: formattedResult.sevenPrizes, isArray: true, minLength: 4 },
                ];

                for (const { key, data, isArray, minLength } of prizeTypes) {
                    if (isArray) {
                        for (const [index, prize] of data.entries()) {
                            if (prize && prize !== '...' && prize !== '****' && /^\d+$/.test(prize)) {
                                const lastPrize = lastPrizeData[key][index] || '...';
                                if (prize !== lastPrize) {
                                    // console.log(`Kết quả mới cho ${key}_${index}: ${prize}`);
                                    publishToRedis(`${key}_${index}`, prize, formattedResult).catch(err => console.error(`Lỗi publish ${key}_${index}:`, err.message));
                                    lastPrizeData[key][index] = prize;
                                }
                            }
                        }
                    } else {
                        if (data && data !== '...' && data !== '') {
                            const lastPrize = lastPrizeData[key] || '...';
                            if (data !== lastPrize) {
                                // console.log(`Kết quả mới cho ${key}: ${data}`);
                                publishToRedis(key, data, formattedResult).catch(err => console.error(`Lỗi publish ${key}:`, err.message));
                                lastPrizeData[key] = data;
                            }
                        }
                    }
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

                if (
                    formattedResult.maDB !== '...' ||
                    formattedResult.specialPrize.some(prize => prize !== '...') ||
                    formattedResult.firstPrize.some(prize => prize !== '...') ||
                    formattedResult.secondPrize.some(prize => prize !== '...') ||
                    formattedResult.threePrizes.some(prize => prize !== '...') ||
                    formattedResult.fourPrizes.some(prize => prize !== '...') ||
                    formattedResult.fivePrizes.some(prize => prize !== '...') ||
                    formattedResult.sixPrizes.some(prize => prize !== '...') ||
                    formattedResult.sevenPrizes.some(prize => prize !== '...')
                ) {
                    await saveToMongoDB(formattedResult);
                } else {
                    // console.log(`Dữ liệu ngày ${date} cho ${station} chưa có, tiếp tục cào...`);
                }

                await logPerformance(iterationStart, iteration, true);
                successCount += 1;

                if (isDataComplete(formattedResult, completedPrizes, stableCounts, startTime)) {
                    console.log(`Dữ liệu ngày ${date} cho ${station} đã đầy đủ, dừng cào.`);
                    isStopped = true;
                    clearInterval(intervalId);

                    const today = formatDateToDDMMYYYY(dateObj);
                    await setRedisExpiration(today);

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
            } catch (error) {
                console.error(`Lỗi khi cào dữ liệu ngày ${date}:`, error.message);
                await logPerformance(iterationStart, iteration, false);
                errorCount += 1;
            }
            // console.log(`Kết thúc lần cào ${iteration} tại: ${(Date.now() - startTime) / 1000} giây`);
        };

        await scrapeAndSave();
        if (!isStopped) {
            intervalId = setInterval(scrapeAndSave, intervalMs);
        }

        setTimeout(async () => {
            if (!isStopped) {
                isStopped = true;
                clearInterval(intervalId);
                // console.log(`Dữ liệu ngày ${date} cho ${station} dừng sau 17 phút do hết thời gian.`);
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

                const today = formatDateToDDMMYYYY(dateObj);
                await setRedisExpiration(today);

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
        const today = formatDateToDDMMYYYY(dateObj || new Date());
        await setRedisExpiration(today);

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