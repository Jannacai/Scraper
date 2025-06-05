const puppeteer = require('puppeteer');
const mongoose = require('mongoose');
const redis = require('redis');
const { lock } = require('proper-lockfile');
const fs = require('fs');
const path = require('path');
const pidusage = require('pidusage');
require('dotenv').config();

const XSMN = require('./src/models/XS_MN.models');

// Kết nối MongoDB với retry
async function connectMongoDB() {
    let retries = 3;
    while (retries > 0) {
        try {
            await mongoose.connect(process.env.MONGODB_URI || 'mongodb://localhost:27017/xsmn', {
                maxPoolSize: 20,
                minPoolSize: 2,
            });
            console.log('Đã kết nối MongoDB');
            return;
        } catch (err) {
            retries -= 1;
            console.error(`Lỗi kết nối MongoDB (còn ${retries} lần thử):`, err.message);
            if (retries === 0) throw err;
            await new Promise(resolve => setTimeout(resolve, 5000));
        }
    }
}
connectMongoDB().catch(err => console.error('Không thể kết nối MongoDB:', err));

// Kết nối Redis với retry
const redisClient = redis.createClient({
    url: process.env.REDIS_URL || 'redis://localhost:6379',
});
async function connectRedis() {
    let retries = 3;
    while (retries > 0) {
        try {
            await redisClient.connect();
            console.log('Đã kết nối Redis');
            return;
        } catch (err) {
            retries -= 1;
            console.error(`Lỗi kết nối Redis (còn ${retries} lần thử):`, err.message);
            if (retries === 0) throw err;
            await new Promise(resolve => setTimeout(resolve, 5000));
        }
    }
}
connectRedis().catch(err => console.error('Không thể kết nối Redis:', err));

// File lock
const lockFilePath = path.resolve(__dirname, 'xsmn_scraper.lock');
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

// Danh sách tỉnh theo ngày
const provincesByDay = {
    1: [
        { tinh: 'tphcm', tentinh: 'TP.HCM' },
        { tinh: 'dong-thap', tentinh: 'Đồng Tháp' },
        { tinh: 'ca-mau', tentinh: 'Cà Mau' }
    ],
    2: [
        { tinh: 'ben-tre', tentinh: 'Bến Tre' },
        { tinh: 'vung-tau', tentinh: 'Vũng Tàu' },
        { tinh: 'bac-lieu', tentinh: 'Bạc Liêu' }
    ],
    3: [
        { tinh: 'dong-nai', tentinh: 'Đồng Nai' },
        { tinh: 'can-tho', tentinh: 'Cần Thơ' },
        { tinh: 'soc-trang', tentinh: 'Sóc Trăng' }
    ],
    4: [
        { tinh: 'tay-ninh', tentinh: 'Tây Ninh' },
        { tinh: 'an-giang', tentinh: 'An Giang' },
        { tinh: 'binh-thuan', tentinh: 'Bình Thuận' }
    ],
    5: [
        { tinh: 'vinh-long', tentinh: 'Vĩnh Long' },
        { tinh: 'binh-duong', tentinh: 'Bình Dương' },
        { tinh: 'tra-vinh', tentinh: 'Trà Vinh' }
    ],
    6: [
        { tinh: 'tphcm', tentinh: 'TP.HCM' },
        { tinh: 'long-an', tentinh: 'Long An' },
        { tinh: 'binh-phuoc', tentinh: 'Bình Phước' },
        { tinh: 'hau-giang', tentinh: 'Hậu Giang' }
    ],
    0: [
        { tinh: 'tien-giang', tentinh: 'Tiền Giang' },
        { tinh: 'kien-giang', tentinh: 'Kiên Giang' },
        { tinh: 'da-lat', tentinh: 'Đà Lạt' }
    ]
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
        result.specialPrize && result.specialPrize.length === 1 && !result.specialPrize.includes('...') &&
        result.firstPrize && result.firstPrize.length === 1 && !result.firstPrize.includes('...') &&
        result.secondPrize && result.secondPrize.length === 1 && !result.secondPrize.includes('...') &&
        result.threePrizes && result.threePrizes.length === 2 && !result.threePrizes.includes('...') &&
        result.fourPrizes && result.fourPrizes.length === 7 && !result.fourPrizes.includes('...') &&
        result.fivePrizes && result.fivePrizes.length === 1 && !result.fivePrizes.includes('...') &&
        result.sixPrizes && result.sixPrizes.length === 3 && !result.sixPrizes.includes('...') &&
        result.sevenPrizes && result.sevenPrizes.length === 1 && !result.sevenPrizes.includes('...') &&
        result.eightPrizes && result.eightPrizes.length === 1 && !result.eightPrizes.includes('...')
    );
}

// Hàm kiểm tra xem có dữ liệu nào đáng lưu không
function hasAnyData(result) {
    return (
        result.tentinh && result.tentinh.length >= 1 && !result.tentinh.startsWith('Tỉnh_') &&
        (
            (result.specialPrize && result.specialPrize.some(prize => prize !== '...')) ||
            (result.firstPrize && result.firstPrize.some(prize => prize !== '...')) ||
            (result.secondPrize && result.secondPrize.some(prize => prize !== '...')) ||
            (result.threePrizes && result.threePrizes.some(prize => prize !== '...')) ||
            (result.fourPrizes && result.fourPrizes.some(prize => prize !== '...')) ||
            (result.fivePrizes && result.fivePrizes.some(prize => prize !== '...')) ||
            (result.sixPrizes && result.sixPrizes.some(prize => prize !== '...')) ||
            (result.sevenPrizes && result.sevenPrizes.some(prize => prize !== '...')) ||
            (result.eightPrizes && result.eightPrizes.some(prize => prize !== '...'))
        )
    );
}

// Hàm publish dữ liệu lên Redis với kiểm tra trùng lặp
async function publishToRedis(prizeType, prizeData, additionalData) {
    const { drawDate, tentinh, tinh, year, month } = additionalData;
    const today = formatDateToDDMMYYYY(new Date(drawDate));
    const redisKey = `kqxs:xsmn:${today}:${tinh}`;
    const message = JSON.stringify({
        prizeType,
        prizeData,
        drawDate: today,
        tentinh,
        tinh,
        year,
        month,
    });

    // Kiểm tra dữ liệu trùng lặp
    try {
        const existingData = await redisClient.hGet(redisKey, prizeType);
        const existingArray = existingData ? JSON.parse(existingData) : [];
        if (JSON.stringify(prizeData) === JSON.stringify(existingArray)) {
            console.log(`Bỏ qua ${prizeType} cho tỉnh ${tentinh}: Dữ liệu không thay đổi`);
            return false;
        }

        console.log(`Chuẩn bị gửi Redis: ${prizeType} cho tỉnh ${tentinh}`, prizeData, `Kênh: xsmn:${today}:${tinh}`);
        if (!redisClient.isOpen) {
            console.log('Redis client chưa sẵn sàng, kết nối lại...');
            await connectRedis();
        }
        await Promise.all([
            redisClient.publish(`xsmn:${today}:${tinh}`, message),
            redisClient.hSet(redisKey, prizeType, JSON.stringify(prizeData)),
            redisClient.hSet(redisKey, 'metadata', JSON.stringify({ tentinh, tinh, year, month })),
            redisClient.expire(redisKey, 7200),
        ]);
        console.log(`Đã gửi ${prizeType} và metadata qua Redis cho ngày ${today}, tỉnh ${tentinh}`);
        return true;
    } catch (error) {
        console.error(`Lỗi gửi Redis (${prizeType}, tỉnh ${tentinh}):`, error.message);
        return false;
    }
}

// Hàm lưu dữ liệu vào MongoDB
async function saveToMongoDB(result) {
    try {
        const dateObj = new Date(result.drawDate);
        const existingResult = await XSMN.findOne({ drawDate: dateObj, station: result.station, tentinh: result.tentinh }).lean();

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
                await XSMN.updateOne(
                    { drawDate: dateObj, station: result.station, tentinh: result.tentinh },
                    { $set: result },
                    { upsert: true }
                );
                console.log(`Cập nhật kết quả ngày ${result.drawDate.toISOString().split('T')[0]} cho tỉnh ${result.tentinh}`);
            } else {
                console.log(`Dữ liệu ngày ${result.drawDate.toISOString().split('T')[0]} cho tỉnh ${result.tentinh} không thay đổi`);
            }
        } else {
            await XSMN.create(result);
            console.log(`Lưu kết quả mới ngày ${result.drawDate.toISOString().split('T')[0]} cho tỉnh ${result.tentinh}`);
        }
    } catch (error) {
        console.error(`Lỗi khi lưu dữ liệu ngày ${result.drawDate.toISOString().split('T')[0]} cho tỉnh ${result.tentinh}:`, error.message);
    }
}

// Hàm ghi log hiệu suất
async function logPerformance(startTime, iteration, success, province) {
    try {
        const stats = await pidusage(process.pid);
        const duration = (Date.now() - startTime) / 1000;
        console.log(`Lần cào ${iteration} cho tỉnh ${province} (${success ? 'Thành công' : 'Thất bại'}):`, {
            duration: `${duration.toFixed(2)}s`,
            cpu: `${stats.cpu.toFixed(2)}%`,
            memory: `${(stats.memory / 1024 / 1024).toFixed(2)}MB`,
        });
    } catch (error) {
        console.error(`Lỗi ghi log hiệu suất:`, error.message);
    }
}

// Hàm cào dữ liệu XSMN
async function scrapeXSMN(date, station, provinces = null) {
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
        if (isNaN(dateObj.getTime())) {
            throw new Error(`Ngày không hợp lệ: ${date}`);
        }
        const formattedDate = date.replace(/\//g, '-');
        const dayOfWeekIndex = dateObj.getDay();
        console.log(`Ngày: ${date}, DayOfWeekIndex: ${dayOfWeekIndex}`);

        // Sử dụng provinces từ tham số hoặc lấy từ provincesByDay
        const targetProvinces = provinces && provinces.length > 0
            ? provinces
            : provincesByDay[dayOfWeekIndex] || [];
        console.log(`Tỉnh mục tiêu:`, targetProvinces);

        if (!targetProvinces || targetProvinces.length === 0) {
            throw new Error(`Không tìm thấy danh sách tỉnh cho ngày ${date}`);
        }

        ensureLockFile();
        release = await lock(lockFilePath, { retries: 5, stale: 10000 });

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
        if (station.toLowerCase() === 'xsmn') {
            baseUrl = `https://xosovn.com/xsmn-${formattedDate}`;
            console.log(`Đang cào dữ liệu từ: ${baseUrl}`);
        } else {
            throw new Error('Chỉ hỗ trợ đài xsmn trong phiên bản này');
        }

        // Dữ liệu placeholder ban đầu cho mỗi tỉnh
        const initialData = {
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

        const scrapeAndSave = async () => {
            iteration += 1;
            const iterationStart = Date.now();
            try {
                const now = new Date();
                const isLiveWindow = now.getHours() === 16 && now.getMinutes() >= 15 && now.getMinutes() <= 35;
                const intervalMs = isLiveWindow ? 3000 : 30000;
                console.log(`Lần cào ${iteration}, isLiveWindow: ${isLiveWindow}, intervalMs: ${intervalMs}`);

                let attempt = 0;
                const maxAttempts = 5;
                let pageLoaded = false;
                while (attempt < maxAttempts) {
                    try {
                        const response = await page.goto(baseUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
                        if (response && response.ok()) {
                            await page.waitForSelector('table.kqsx-mt', { timeout: 10000 }).catch(() => {
                                console.log('Chưa thấy bảng kết quả, tiếp tục cào...');
                            });
                            pageLoaded = true;
                            break;
                        }
                        throw new Error(`HTTP status: ${response?.status()}`);
                    } catch (error) {
                        attempt++;
                        console.warn(`Lỗi request lần ${attempt}: ${error.message}. Thử lại...`);
                        if (attempt === maxAttempts) {
                            throw new Error(`Không thể tải trang sau ${maxAttempts} lần thử: ${error.message}`);
                        }
                        await new Promise(resolve => setTimeout(resolve, 2000));
                    }
                }

                if (!pageLoaded) {
                    console.log('Không thể tải trang, bỏ qua lần cào này.');
                    await logPerformance(iterationStart, iteration, false, 'N/A');
                    errorCount += 1;
                    return;
                }

                const results = await page.evaluate(() => {
                    const getPrizes = (selector, provinceIndex) => {
                        const elements = document.querySelectorAll(`table.kqsx-mt tr td:nth-child(${provinceIndex + 2}) ${selector}`);
                        return Array.from(elements).map(elem => elem.textContent.trim()).filter(prize => prize);
                    };

                    const provinceRow = document.querySelector('table.kqsx-mt tr.bg-pr');
                    if (!provinceRow) return { provinces: [], provinceData: {} };
                    const provinces = Array.from(provinceRow.querySelectorAll('th')).slice(1).map(th => th.querySelector('a')?.textContent.trim()).filter(Boolean);

                    const provinceData = {};
                    provinces.forEach((province, index) => {
                        provinceData[province] = {
                            drawDate: document.querySelector('.ngay_quay')?.textContent.trim() || '',
                            specialPrize: getPrizes('span[class*="v-gdb"]', index),
                            firstPrize: getPrizes('span[class*="v-g1"]', index),
                            secondPrize: getPrizes('span[class*="v-g2"]', index),
                            threePrizes: getPrizes('span[class*="v-g3"]', index),
                            fourPrizes: getPrizes('span[class*="v-g4-"]', index),
                            fivePrizes: getPrizes('span[class*="v-g5"]', index),
                            sixPrizes: getPrizes('span[class*="v-g6"]', index),
                            sevenPrizes: getPrizes('span[class*="v-g7"]', index),
                            eightPrizes: getPrizes('span[class*="v-g8"]', index),
                        };
                    });

                    return { provinces, provinceData };
                });

                if (results.provinces.length === 0) {
                    console.log('Không tìm thấy tỉnh nào trong bảng kết quả.');
                    await logPerformance(iterationStart, iteration, false, 'N/A');
                    errorCount += 1;
                    return;
                }

                // Khởi tạo lastPrizeDataByProvince
                targetProvinces.forEach(province => {
                    if (!lastPrizeDataByProvince[province.tentinh]) {
                        lastPrizeDataByProvince[province.tentinh] = { ...initialData };
                    }
                });

                const drawDate = results.provinceData[results.provinces[0]]?.drawDate.match(/\d{2}\/\d{2}\/\d{4}/)?.[0] || date;
                const dateObj = new Date(drawDate.split('/').reverse().join('-'));
                const daysOfWeek = ['Chủ nhật', 'Thứ 2', 'Thứ 3', 'Thứ 4', 'Thứ 5', 'Thứ 6', 'Thứ 7'];
                const dayOfWeek = daysOfWeek[dateObj.getDay()];
                console.log(`Ngày quay: ${drawDate}, DayOfWeek: ${dayOfWeek}`);

                let allProvincesComplete = true;

                for (const { tentinh, tinh } of targetProvinces) {
                    if (!results.provinceData[tentinh]) {
                        console.warn(`Không tìm thấy dữ liệu cho tỉnh ${tentinh}, bỏ qua.`);
                        continue;
                    }
                    const slug = `xsmn-${formattedDate}-${tinh}`;
                    const provinceData = results.provinceData[tentinh] || {};

                    const formattedResult = {
                        drawDate: dateObj,
                        slug,
                        year: dateObj.getFullYear(),
                        month: dateObj.getMonth() + 1,
                        dayOfWeek,
                        tentinh,
                        tinh,
                        specialPrize: provinceData.specialPrize?.length ? provinceData.specialPrize : lastPrizeDataByProvince[tentinh].specialPrize,
                        firstPrize: provinceData.firstPrize?.length ? provinceData.firstPrize : lastPrizeDataByProvince[tentinh].firstPrize,
                        secondPrize: provinceData.secondPrize?.length ? provinceData.secondPrize : lastPrizeDataByProvince[tentinh].secondPrize,
                        threePrizes: provinceData.threePrizes?.length ? provinceData.threePrizes : lastPrizeDataByProvince[tentinh].threePrizes,
                        fourPrizes: provinceData.fourPrizes?.length ? provinceData.fourPrizes : lastPrizeDataByProvince[tentinh].fourPrizes,
                        fivePrizes: provinceData.fivePrizes?.length ? provinceData.fivePrizes : lastPrizeDataByProvince[tentinh].fivePrizes,
                        sixPrizes: provinceData.sixPrizes?.length ? provinceData.sixPrizes : lastPrizeDataByProvince[tentinh].sixPrizes,
                        sevenPrizes: provinceData.sevenPrizes?.length ? provinceData.sevenPrizes : lastPrizeDataByProvince[tentinh].sevenPrizes,
                        eightPrizes: provinceData.eightPrizes?.length ? provinceData.eightPrizes : lastPrizeDataByProvince[tentinh].eightPrizes,
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

                    // Publish các giải với kiểm tra trùng lặp
                    const publishPromises = prizeTypes.map(async ({ key, data }) => {
                        const sent = await publishToRedis(key, data, formattedResult);
                        if (sent) {
                            lastPrizeDataByProvince[tentinh][key] = data;
                        }
                    });

                    // Publish từng phần tử riêng lẻ nếu có dữ liệu mới
                    for (const { key, data, isArray } of prizeTypes) {
                        if (isArray) {
                            data.forEach(async (prize, index) => {
                                const lastPrize = lastPrizeDataByProvince[tentinh][key][index] || '...';
                                if (prize !== lastPrize && prize !== '...') {
                                    console.log(`Kết quả mới cho ${key}_${index} (tỉnh ${tentinh}): ${prize}`);
                                    const sent = await publishToRedis(`${key}_${index}`, prize, formattedResult);
                                    if (sent) {
                                        lastPrizeDataByProvince[tentinh][key][index] = prize;
                                    }
                                }
                            });
                        }
                    }

                    await Promise.all(publishPromises);

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
                console.error(`Lỗi khi cào dữ liệu lần ${iteration} ngày ${date}:`, error.message);
                await logPerformance(iterationStart, iteration, false, 'N/A');
                errorCount += 1;
                if (errorCount >= 10) {
                    console.log(`Quá nhiều lỗi (${errorCount}), dừng cào.`);
                    clearInterval(intervalId);
                    await page.close();
                    await browser.close();
                    await release();
                    return;
                }
            }
        };

        // Publish initialData ngay khi bắt đầu
        for (const { tentinh, tinh } of targetProvinces) {
            lastPrizeDataByProvince[tentinh] = { ...initialData };
            const formattedResult = {
                drawDate: dateObj,
                slug: `xsmn-${formattedDate}-${tinh}`,
                year: dateObj.getFullYear(),
                month: dateObj.getMonth() + 1,
                dayOfWeek: ['Chủ nhật', 'Thứ 2', 'Thứ 3', 'Thứ 4', 'Thứ 5', 'Thứ 6', 'Thứ 7'][dateObj.getDay()],
                tentinh,
                tinh,
                ...initialData,
                station,
                createdAt: new Date(),
            };
            const prizeTypes = [
                { key: 'specialPrize', data: initialData.specialPrize },
                { key: 'firstPrize', data: initialData.firstPrize },
                { key: 'secondPrize', data: initialData.secondPrize },
                { key: 'threePrizes', data: initialData.threePrizes },
                { key: 'fourPrizes', data: initialData.fourPrizes },
                { key: 'fivePrizes', data: initialData.fivePrizes },
                { key: 'sixPrizes', data: initialData.sixPrizes },
                { key: 'sevenPrizes', data: initialData.sevenPrizes },
                { key: 'eightPrizes', data: initialData.eightPrizes },
            ];
            await Promise.all(prizeTypes.map(async ({ key, data }) => {
                await publishToRedis(key, data, formattedResult);
            }));
        }

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
        }, 23 * 60 * 1000);

    } catch (error) {
        console.error(`Lỗi khi khởi động scraper ngày ${date}:`, error.message);
        if (page) await page.close();
        if (browser) await browser.close();
        if (release) await release();
    }
}

module.exports = { scrapeXSMN };

// Chạy thủ công nếu có tham số dòng lệnh
const [, , date, station] = process.argv;
if (date && station) {
    console.log(`Chạy thủ công cho ngày ${date} và đài ${station}`);
    scrapeXSMN(date, station);
} else {
    console.log('Nếu muốn chạy thủ công, dùng lệnh: node xsmn_scraper.js 06/05/2025 xsmn');
}

// Đóng kết nối khi dừng chương trình
process.on('SIGINT', async () => {
    await mongoose.connection.close();
    await redisClient.quit();
    console.log('Đã đóng kết nối MongoDB và Redis');
    process.exit(0);
});