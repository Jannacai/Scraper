const puppeteer = require('puppeteer-core');
const chrome = require('chrome-aws-lambda');
const mongoose = require('mongoose');
const redis = require('redis');
const Redlock = require('redlock');
const randomUserAgent = require('random-useragent');
const winston = require('winston');
require('dotenv').config();

const XSMB = require('./src/models/XS_MB.models');

const logger = winston.createLogger({
    transports: [
        new winston.transports.File({ filename: 'scraper.log' }),
        new winston.transports.Console(),
    ],
});

mongoose.connect(process.env.MONGODB_URI, {
    maxPoolSize: 10,
    minPoolSize: 2,
    retryWrites: true,
}).then(() => logger.info('Connected to MongoDB')).catch(err => logger.error('MongoDB error:', err));

const redisClient = redis.createClient({
    url: process.env.REDIS_URL,
    retry_strategy: (options) => {
        if (options.attempt > 3) return new Error('Redis retry exhausted');
        return Math.min(options.attempt * 100, 3000);
    },
});
redisClient.connect().catch(err => logger.error('Redis error:', err));

const redlock = new Redlock([redisClient], { retryCount: 3, retryDelay: 200 });

async function publishToRedis(prizeType, prizeData, additionalData) {
    const { drawDate, tentinh, tinh, year, month } = additionalData;
    const today = new Date(drawDate).toLocaleDateString('vi-VN', {
        day: '2-digit',
        month: '2-digit',
        year: 'numeric',
    }).replace(/\//g, '-');
    const message = JSON.stringify({ prizeType, prizeData, drawDate: today, tentinh, tinh, year, month });

    try {
        const pipeline = redisClient.pipeline();
        pipeline.publish(`xsmb:${today}`, message);
        pipeline.hSet(`kqxs:${today}`, prizeType, JSON.stringify(prizeData));
        pipeline.hSet(`kqxs:${today}:meta`, 'metadata', JSON.stringify({ tentinh, tinh, year, month }));
        pipeline.expire(`kqxs:${today}`, 3600);
        pipeline.expire(`kqxs:${today}:meta`, 3600);
        await pipeline.exec();
        logger.info(`Published ${prizeType} to Redis for ${today}`);
    } catch (error) {
        logger.error(`Redis publish error (${prizeType}):`, error.message);
        throw error;
    }
}

async function saveToMongoDB(result) {
    try {
        const filter = { drawDate: result.drawDate, station: result.station };
        await XSMB.bulkWrite([
            {
                updateOne: {
                    filter,
                    update: { $set: result },
                    upsert: true,
                },
            },
        ]);
        logger.info(`Saved/updated result for ${result.drawDate.toISOString().split('T')[0]}`);
    } catch (error) {
        logger.error(`MongoDB save error: ${error.message}`);
    }
}

async function scrapeXSMB(date, station) {
    let browser;
    let page;
    let intervalId;
    let lock;
    try {
        const dateParts = date.split('/');
        const dateObj = new Date(`${dateParts[2]}-${dateParts[1]}-${dateParts[0]}`);
        const formattedDate = date.replace(/\//g, '-');

        lock = await redlock.acquire(`lock:scraper:${station}:${date}`, 10000);
        logger.info('Acquired scraper lock');

        browser = await chrome.puppeteer.launch({
            args: chrome.args,
            executablePath: await chrome.executablePath,
            headless: true,
            defaultViewport: { width: 1280, height: 720 },
        });
        page = await browser.newPage();
        await page.setUserAgent(randomUserAgent.getRandom());
        await page.setRequestInterception(true);
        page.on('request', (req) => {
            if (['image', 'stylesheet', 'font', 'script', 'media'].includes(req.resourceType())) {
                req.abort();
            } else {
                req.continue();
            }
        });

        let baseUrl, dateHash;
        if (station.toLowerCase() === 'xsmb') {
            baseUrl = `https://xosovn.com/xsmb-${formattedDate}`;
            dateHash = `#kqngay_${formattedDate.split('-').join('')}`;
            logger.info(`Scraping from: ${baseUrl}`);
        } else {
            throw new Error('Only xsmb supported');
        }

        const scrapeAndSave = async () => {
            try {
                const now = new Date();
                const isLiveWindow = now.getHours() === 18 && now.getMinutes() >= 15 && now.getMinutes() <= 35;
                const intervalMs = isLiveWindow ? 5000 : 30000;

                let attempt = 0;
                const maxAttempts = 3;
                while (attempt < maxAttempts) {
                    try {
                        await page.goto(baseUrl, { waitUntil: 'domcontentloaded', timeout: 15000 });
                        await page.waitForSelector(`${dateHash} span[class*="v-madb"]`, { timeout: 5000 }).catch(() => {
                            logger.info('No maDB yet, continuing...');
                        });
                        break;
                    } catch (error) {
                        attempt++;
                        logger.warn(`Request error attempt ${attempt}: ${error.message}`);
                        if (attempt === maxAttempts) throw error;
                        await new Promise(resolve => setTimeout(resolve, 1000 * Math.pow(2, attempt)));
                    }
                }

                const result = await page.evaluate((dateHash) => {
                    const getPrizes = (selector) => {
                        const elements = document.querySelectorAll(selector);
                        return Array.from(elements).map(elem => elem.textContent.trim()).filter(prize => prize);
                    };

                    const maDB = document.querySelector(`${dateHash} span[class*="v-madb"]`)?.textContent.trim() ||
                        document.querySelector(`${dateHash} [class*="madb"]`)?.textContent.trim() || '...';

                    return {
                        drawDate: document.querySelector('.ngay_quay')?.textContent.trim() || '',
                        maDB,
                        specialPrize: getPrizes(`${dateHash} span[class*="v-gdb"]`),
                        firstPrize: getPrizes(`${dateHash} span[class*="v-g1"]`),
                        secondPrize: getPrizes(`${dateHash} span[class*="v-g2-"]`),
                        threePrizes: getPrizes(`${dateHash} span[class*="v-g3-"]`),
                        fourPrizes: getPrizes(`${dateHash} span[class*="v-g4-"]`),
                        fivePrizes: getPrizes(`${dateHash} span[class*="v-g5-"]`),
                        sixPrizes: getPrizes(`${dateHash} span[class*="v-g6-"]`),
                        sevenPrizes: getPrizes(`${dateHash} span[class*="v-g7-"]`),
                    };
                }, dateHash);

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
                    default: throw new Error('Invalid day of week');
                }

                const slug = `${station}-${formattedDate}`;
                const dayOfWeek = ['Chủ nhật', 'Thứ 2', 'Thứ 3', 'Thứ 4', 'Thứ 5', 'Thứ 6', 'SecondPrize'][dayOfWeekIndex];

                const formattedResult = {
                    drawDate: dateObj,
                    slug,
                    year: dateObj.getFullYear(),
                    month: dateObj.getMonth() + 1,
                    dayOfWeek,
                    maDB: result.maDB || '...',
                    tentinh,
                    tinh,
                    specialPrize: result.specialPrize.length ? result.specialPrize : ['...'],
                    firstPrize: result.firstPrize.length ? result.firstPrize : ['...'],
                    secondPrize: result.secondPrize.length ? result.secondPrize : ['...', '...'],
                    threePrizes: result.threePrizes.length ? result.threePrizes : ['...', '...', '...', '...', '...', '...'],
                    fourPrizes: result.fourPrizes.length ? result.fourPrizes : ['...', '...', '...', '...'],
                    fivePrizes: result.fivePrizes.length ? result.fivePrizes : ['...', '...', '...', '...', '...', '...'],
                    sixPrizes: result.sixPrizes.length ? result.sixPrizes : ['...', '...', '...'],
                    sevenPrizes: result.sevenPrizes.length ? result.sevenPrizes : ['...', '...', '...', '...'],
                    station,
                    createdAt: new Date(),
                };

                const prizeTypes = [
                    { key: 'maDB', data: formattedResult.maDB },
                    { key: 'specialPrize', data: formattedResult.specialPrize },
                    { key: 'firstPrize', data: formattedResult.firstPrize },
                    { key: 'secondPrize', data: formattedResult.secondPrize },
                    { key: 'threePrizes', data: formattedResult.threePrizes },
                    { key: 'fourPrizes', data: formattedResult.fourPrizes },
                    { key: 'fivePrizes', data: formattedResult.fivePrizes },
                    { key: 'sixPrizes', data: formattedResult.sixPrizes },
                    { key: 'sevenPrizes', data: formattedResult.sevenPrizes },
                ];

                for (const { key, data } of prizeTypes) {
                    if (data && !data.includes('...') && data !== '****') {
                        await publishToRedis(key, data, formattedResult);
                    }
                }

                if (
                    formattedResult.maDB !== '...' ||
                    formattedResult.specialPrize.some(prize => prize !== '...') ||
                    formattedResult.firstPrize.some(prize => prize !== '...')
                ) {
                    await saveToMongoDB(formattedResult);
                } else {
                    logger.info(`Data for ${date} (${station}) not ready, continuing...`);
                }

                if (isDataComplete(formattedResult)) {
                    logger.info(`Data for ${date} (${station}) complete, stopping.`);
                    clearInterval(intervalId);
                    await page.close();
                    await browser.close();
                    await lock.release();
                    return;
                }

                clearInterval(intervalId);
                intervalId = setInterval(scrapeAndSave, intervalMs);
            } catch (error) {
                logger.error(`Scrape error for ${date}: ${error.message}`);
            }
        };

        intervalId = setInterval(scrapeAndSave, 5000);

        setTimeout(async () => {
            clearInterval(intervalId);
            logger.info(`Stopped scraping for ${date} after 20 minutes`);
            if (page) await page.close();
            if (browser) await browser.close();
            if (lock) await lock.release();
        }, 20 * 60 * 1000);

    } catch (error) {
        logger.error(`Scraper startup error for ${date}: ${error.message}`);
        if (page) await page.close();
        if (browser) await browser.close();
        if (lock) await lock.release();
    }
}

function isDataComplete(result) {
    const isValidMaDB = result.maDB && result.maDB !== '...' && /^\d{5}$/.test(result.maDB);
    return (
        isValidMaDB &&
        result.tentinh && result.tentinh.length >= 1 &&
        result.specialPrize && result.specialPrize.length >= 1 && !result.specialPrize.includes('...') &&
        result.firstPrize && result.firstPrize.length >= 1 && !result.firstPrize.includes('...') &&
        result.secondPrize && result.secondPrize.length >= 2 && !result.secondPrize.includes('...') &&
        result.threePrizes && result.threePrizes.length >= 6 && !result.threePrizes.includes('...') &&
        result.fourPrizes && result.fourPrizes.length >= 4 && !result.fourPrizes.includes('...') &&
        result.fivePrizes && result.fivePrizes.length >= 6 && !result.fivePrizes.includes('...') &&
        result.sixPrizes && result.sixPrizes.length >= 3 && !result.sixPrizes.includes('...') &&
        result.sevenPrizes && result.sevenPrizes.length >= 4 && !result.sevenPrizes.includes('...')
    );
}

module.exports = { scrapeXSMB, redisClient };