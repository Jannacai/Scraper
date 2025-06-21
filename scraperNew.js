// const puppeteer = require('puppeteer');
// const mongoose = require('mongoose');
// const { lock } = require('proper-lockfile');
// const fs = require('fs');
// const path = require('path');
// const redis = require('redis');
// const pidusage = require('pidusage');
// require('dotenv').config();

// const XSMB = require('./src/models/XS_MB.models');
// // Đã sửa timeOut từ 3s xuống 2s
// // Đã sửa "waitForSelector" từ 8s-6s, từ 3s-2s
// mongoose.connect(process.env.MONGODB_URI || 'mongodb://localhost:27017/xsmb', {
//     maxPoolSize: 20,
//     minPoolSize: 2,
// }).then(() => console.log('Đã kết nối MongoDB')).catch(err => console.error('Lỗi kết nối MongoDB:', err));

// const redisClient = redis.createClient({
//     url: process.env.REDIS_URL || 'redis://localhost:6379',
// });
// redisClient.connect().catch(err => console.error('Lỗi kết nối Redis:', err));

// const lockFilePath = path.resolve(__dirname, 'scraper.lock');
// const ensureLockFile = () => {
//     try {
//         if (!fs.existsSync(lockFilePath)) {
//             fs.writeFileSync(lockFilePath, '');
//             console.log(`Tạo file ${lockFilePath}`);
//         }
//     } catch (error) {
//         console.error(`Lỗi khi tạo file ${lockFilePath}:`, error.message);
//         throw error;
//     }
// };

// function formatDateToDDMMYYYY(date) {
//     const day = String(date.getDate()).padStart(2, '0');
//     const month = String(date.getMonth() + 1).padStart(2, '0');
//     const year = date.getFullYear();
//     return `${day}-${month}-${year}`;
// }

// function isDataComplete(result, completedPrizes) {
//     const isValidMaDB = result.maDB && result.maDB !== '...';
//     if (!isValidMaDB) {
//         console.log('maDB không hợp lệ:', result.maDB);
//     }
//     completedPrizes.maDB = isValidMaDB;
//     completedPrizes.specialPrize = result.specialPrize && result.specialPrize.length >= 1 && !result.specialPrize.includes('...');
//     completedPrizes.firstPrize = result.firstPrize && result.firstPrize.length >= 1 && !result.firstPrize.includes('...');
//     completedPrizes.secondPrize = result.secondPrize && result.secondPrize.length >= 2 && !result.secondPrize.includes('...');
//     completedPrizes.threePrizes = result.threePrizes && result.threePrizes.length >= 6 && !result.threePrizes.includes('...');
//     completedPrizes.fourPrizes = result.fourPrizes && result.fourPrizes.length >= 4 && !result.fourPrizes.includes('...');
//     completedPrizes.fivePrizes = result.fivePrizes && result.fivePrizes.length >= 6 && !result.fivePrizes.includes('...');
//     completedPrizes.sixPrizes = result.sixPrizes && result.sixPrizes.length >= 3 && !result.sixPrizes.includes('...');
//     completedPrizes.sevenPrizes = result.sevenPrizes && result.sevenPrizes.length >= 4 && !result.sevenPrizes.includes('...');

//     return (
//         isValidMaDB &&
//         result.tentinh && result.tentinh.length >= 1 &&
//         result.specialPrize && result.specialPrize.length >= 1 && !result.specialPrize.includes('...') &&
//         result.firstPrize && result.firstPrize.length >= 1 && !result.firstPrize.includes('...') &&
//         result.secondPrize && result.secondPrize.length >= 2 && !result.secondPrize.includes('...') &&
//         result.threePrizes && result.threePrizes.length >= 6 && !result.threePrizes.includes('...') &&
//         result.fourPrizes && result.fourPrizes.length >= 4 && !result.fourPrizes.includes('...') &&
//         result.fivePrizes && result.fivePrizes.length >= 6 && !result.fivePrizes.includes('...') &&
//         result.sixPrizes && result.sixPrizes.length >= 3 && !result.sixPrizes.includes('...') &&
//         result.sevenPrizes && result.sevenPrizes.length >= 4 && !result.sevenPrizes.includes('...')
//     );
// }

// async function publishToRedis(prizeType, prizeData, additionalData) {
//     const { drawDate, tentinh, tinh, year, month } = additionalData;
//     const today = formatDateToDDMMYYYY(new Date(drawDate));
//     const message = JSON.stringify({
//         prizeType,
//         prizeData,
//         drawDate: today,
//         tentinh,
//         tinh,
//         year,
//         month,
//     });
//     console.log(`Chuẩn bị gửi Redis: ${prizeType}`, prizeData, `Kênh: xsmb:${today}`);

//     try {
//         if (!redisClient.isOpen) {
//             console.log('Redis client chưa sẵn sàng, kết nối lại...');
//             await redisClient.connect();
//             console.log('Kết nối Redis thành công');
//         }
//         await Promise.all([
//             redisClient.publish(`xsmb:${today}`, message),
//             redisClient.hSet(`kqxs:${today}`, prizeType, JSON.stringify(prizeData)),
//             redisClient.hSet(`kqxs:${today}:meta`, 'metadata', JSON.stringify({ tentinh, tinh, year, month })),
//             redisClient.expire(`kqxs:${today}`, 7200),
//             redisClient.expire(`kqxs:${today}:meta`, 7200),
//         ]);
//         console.log(`Đã gửi ${prizeType} và metadata qua Redis cho ngày ${today}`);
//     } catch (error) {
//         console.error(`Lỗi gửi Redis (${prizeType}):`, error.message);
//         throw error;
//     }
// }

// async function saveToMongoDB(result) {
//     try {
//         const existingResult = await XSMB.findOne({ drawDate: result.drawDate, station: result.station }).lean();

//         if (existingResult) {
//             const existingData = {
//                 maDB: existingResult.maDB,
//                 specialPrize: existingResult.specialPrize,
//                 firstPrize: existingResult.firstPrize,
//                 secondPrize: existingResult.secondPrize,
//                 threePrizes: existingResult.threePrizes,
//                 fourPrizes: existingResult.fourPrizes,
//                 fivePrizes: existingResult.fivePrizes,
//                 sixPrizes: existingResult.sixPrizes,
//                 sevenPrizes: existingResult.sevenPrizes,
//             };

//             const newData = {
//                 maDB: result.maDB,
//                 specialPrize: result.specialPrize,
//                 firstPrize: result.firstPrize,
//                 secondPrize: result.secondPrize,
//                 threePrizes: result.threePrizes,
//                 fourPrizes: result.fourPrizes,
//                 fivePrizes: result.fivePrizes,
//                 sixPrizes: result.sixPrizes,
//                 sevenPrizes: result.sevenPrizes,
//             };

//             if (JSON.stringify(existingData) !== JSON.stringify(newData)) {
//                 await XSMB.updateOne(
//                     { drawDate: result.drawDate, station: result.station },
//                     { $set: result },
//                     { upsert: true }
//                 );
//                 console.log(`Cập nhật kết quả ngày ${result.drawDate.toISOString().split('T')[0]} cho ${result.station}`);
//             } else {
//                 console.log(`Dữ liệu ngày ${result.drawDate.toISOString().split('T')[0]} cho ${result.station} không thay đổi`);
//             }
//         } else {
//             await XSMB.create(result);
//             console.log(`Lưu kết quả mới ngày ${result.drawDate.toISOString().split('T')[0]} cho ${result.station}`);
//         }
//     } catch (error) {
//         console.error(`Lỗi khi lưu dữ liệu ngày ${result.drawDate.toISOString().split('T')[0]}:`, error.message);
//     }
// }

// async function logPerformance(startTime, iteration, success) {
//     const stats = await pidusage(process.pid);
//     const duration = (Date.now() - startTime) / 1000;
//     console.log(`Lần cào ${iteration} (${success ? 'Thành công' : 'Thất bại'}):`, {
//         duration: `${duration.toFixed(2)}s`,
//         cpu: `${stats.cpu.toFixed(2)}%`,
//         memory: `${(stats.memory / 1024 / 1024).toFixed(2)}MB`,
//     });
// }

// async function scrapeXSMB(date, station) {
//     let browser;
//     let page;
//     let intervalId;
//     let release;
//     let isStopped = false;
//     let iteration = 0;
//     let successCount = 0;
//     let errorCount = 0;
//     const startTime = Date.now();
//     const lastPrizeData = {
//         maDB: '...',
//         specialPrize: ['...'],
//         firstPrize: ['...'],
//         secondPrize: ['...', '...'],
//         threePrizes: ['...', '...', '...', '...', '...', '...'],
//         fourPrizes: ['...', '...', '...', '...'],
//         fivePrizes: ['...', '...', '...', '...', '...', '...'],
//         sixPrizes: ['...', '...', '...'],
//         sevenPrizes: ['...', '...', '...', '...'],
//     };
//     const prevPrizeData = JSON.parse(JSON.stringify(lastPrizeData));
//     const completedPrizes = {
//         maDB: false,
//         specialPrize: false,
//         firstPrize: false,
//         secondPrize: false,
//         threePrizes: false,
//         fourPrizes: false,
//         fivePrizes: false,
//         sixPrizes: false,
//         sevenPrizes: false,
//     };

//     try {
//         const dateParts = date.split('/');
//         const dateObj = new Date(`${dateParts[2]}-${dateParts[1]}-${dateParts[0]}`);
//         const formattedDate = date.replace(/\//g, '-');

//         ensureLockFile();
//         release = await lock(lockFilePath, { retries: 3, stale: 10000 });

//         browser = await puppeteer.launch({
//             headless: 'new',
//             args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
//             executablePath: process.env.CHROMIUM_PATH || undefined,
//         });
//         page = await browser.newPage();
//         await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124');

//         let baseUrl, dateHash;
//         if (station.toLowerCase() === 'xsmb') {
//             baseUrl = `https://xosovn.com/xsmb-${formattedDate}`;
//             dateHash = `#kqngay_${formattedDate.split('-').join('')}`;
//             console.log(`Đang cào dữ liệu từ: ${baseUrl}`);
//         } else {
//             throw new Error('Chỉ hỗ trợ đài xsmb trong phiên bản này');
//         }

//         const scrapeAndSave = async () => {
//             if (isStopped || (page && page.isClosed())) {
//                 console.log(`Scraper đã dừng hoặc page đã đóng, bỏ qua lần cào ${iteration + 1}`);
//                 return;
//             }

//             iteration += 1;
//             const iterationStart = Date.now();
//             try {
//                 const now = new Date();
//                 const isLiveWindow = now.getHours() === 18 && now.getMinutes() >= 15 && now.getMinutes() <= 35;
//                 const intervalMs = isLiveWindow ? 1500 : 30000;

//                 let attempt = 0;
//                 const maxAttempts = 3;
//                 while (attempt < maxAttempts) {
//                     try {
//                         await page.goto(baseUrl, { waitUntil: 'domcontentloaded', timeout: 6000 });
//                         await page.waitForSelector(`${dateHash} span[class*="v-madb"]`, { timeout: 2000 }).catch(() => {
//                             console.log('Chưa thấy maDB, tiếp tục cào...');
//                         });
//                         break;
//                     } catch (error) {
//                         attempt++;
//                         console.warn(`Lỗi request lần ${attempt}: ${error.message}. Thử lại...`);
//                         if (attempt === maxAttempts) throw error;
//                         await new Promise(resolve => setTimeout(resolve, 1000));
//                     }
//                 }

//                 const result = await page.evaluate((dateHash, completedPrizes) => {
//                     const getPrizes = (selector) => {
//                         const elements = document.querySelectorAll(selector);
//                         return Array.from(elements).map(elem => elem.textContent.trim()).filter(prize => prize);
//                     };

//                     const maDB = completedPrizes.maDB ? '...' : (
//                         document.querySelector(`${dateHash} span[class*="v-madb"]`)?.textContent.trim() ||
//                         document.querySelector(`${dateHash} [class*="madb"]`)?.textContent.trim() ||
//                         document.querySelector(`${dateHash} .madb`)?.textContent.trim() || '...'
//                     );

//                     return {
//                         drawDate: document.querySelector('.ngay_quay')?.textContent.trim() || '',
//                         maDB,
//                         specialPrize: completedPrizes.specialPrize ? [] : getPrizes(`${dateHash} span[class*="v-gdb"]`),
//                         firstPrize: completedPrizes.firstPrize ? [] : getPrizes(`${dateHash} span[class*="v-g1"]`),
//                         secondPrize: completedPrizes.secondPrize ? [] : getPrizes(`${dateHash} span[class*="v-g2-"]`),
//                         threePrizes: completedPrizes.threePrizes ? [] : getPrizes(`${dateHash} span[class*="v-g3-"]`),
//                         fourPrizes: completedPrizes.fourPrizes ? [] : getPrizes(`${dateHash} span[class*="v-g4-"]`),
//                         fivePrizes: completedPrizes.fivePrizes ? [] : getPrizes(`${dateHash} span[class*="v-g5-"]`),
//                         sixPrizes: completedPrizes.sixPrizes ? [] : getPrizes(`${dateHash} span[class*="v-g6-"]`),
//                         sevenPrizes: completedPrizes.sevenPrizes ? [] : getPrizes(`${dateHash} span[class*="v-g7-"]`),
//                     };
//                 }, dateHash, completedPrizes);

//                 const dayOfWeekIndex = dateObj.getDay();
//                 let tinh, tentinh;
//                 switch (dayOfWeekIndex) {
//                     case 0: tinh = 'thai-binh'; tentinh = 'Thái Bình'; break;
//                     case 1: tinh = 'ha-noi'; tentinh = 'Hà Nội'; break;
//                     case 2: tinh = 'quang-ninh'; tentinh = 'Quảng Ninh'; break;
//                     case 3: tinh = 'bac-ninh'; tentinh = 'Bắc Ninh'; break;
//                     case 4: tinh = 'ha-noi'; tentinh = 'Hà Nội'; break;
//                     case 5: tinh = 'hai-phong'; tentinh = 'Hải Phòng'; break;
//                     case 6: tinh = 'nam-dinh'; tentinh = 'Nam Định'; break;
//                     default: throw new Error('Không xác định được ngày trong tuần');
//                 }

//                 const slug = `${station}-${formattedDate}`;
//                 const dayOfWeek = ['Chủ nhật', 'Thứ 2', 'Thứ 3', 'Thứ 4', 'Thứ 5', 'Thứ 6', 'Thứ 7'][dayOfWeekIndex];

//                 const formattedResult = {
//                     drawDate: dateObj,
//                     slug,
//                     year: dateObj.getFullYear(),
//                     month: dateObj.getMonth() + 1,
//                     dayOfWeek,
//                     maDB: result.maDB || lastPrizeData.maDB,
//                     tentinh,
//                     tinh,
//                     specialPrize: result.specialPrize?.length ? result.specialPrize : lastPrizeData.specialPrize,
//                     firstPrize: result.firstPrize?.length ? result.firstPrize : lastPrizeData.firstPrize,
//                     secondPrize: result.secondPrize?.length ? result.secondPrize : lastPrizeData.secondPrize,
//                     threePrizes: result.threePrizes?.length ? result.threePrizes : lastPrizeData.threePrizes,
//                     fourPrizes: result.fourPrizes?.length ? result.fourPrizes : lastPrizeData.fourPrizes,
//                     fivePrizes: result.fivePrizes?.length ? result.fivePrizes : lastPrizeData.fivePrizes,
//                     sixPrizes: result.sixPrizes?.length ? result.sixPrizes : lastPrizeData.sixPrizes,
//                     sevenPrizes: result.sevenPrizes?.length ? result.sevenPrizes : lastPrizeData.sevenPrizes,
//                     station,
//                     createdAt: new Date(),
//                 };

//                 const prizeTypes = [
//                     { key: 'maDB', data: formattedResult.maDB, isArray: false, minLength: 1 },
//                     { key: 'specialPrize', data: formattedResult.specialPrize, isArray: true, minLength: 1 },
//                     { key: 'firstPrize', data: formattedResult.firstPrize, isArray: true, minLength: 1 },
//                     { key: 'secondPrize', data: formattedResult.secondPrize, isArray: true, minLength: 2 },
//                     { key: 'threePrizes', data: formattedResult.threePrizes, isArray: true, minLength: 6 },
//                     { key: 'fourPrizes', data: formattedResult.fourPrizes, isArray: true, minLength: 4 },
//                     { key: 'fivePrizes', data: formattedResult.fivePrizes, isArray: true, minLength: 6 },
//                     { key: 'sixPrizes', data: formattedResult.sixPrizes, isArray: true, minLength: 3 },
//                     { key: 'sevenPrizes', data: formattedResult.sevenPrizes, isArray: true, minLength: 4 },
//                 ];

//                 // Lưu trạng thái trước của lastPrizeData
//                 Object.assign(prevPrizeData, JSON.parse(JSON.stringify(lastPrizeData)));

//                 // Cập nhật và publish số riêng lẻ song song
//                 const individualPromises = [];
//                 for (const { key, data, isArray } of prizeTypes) {
//                     if (isArray && !completedPrizes[key]) {
//                         for (const [index, prize] of data.entries()) {
//                             if (prize && prize !== '...' && prize !== '****') {
//                                 const lastPrize = lastPrizeData[key][index] || '...';
//                                 if (prize !== lastPrize) {
//                                     console.log(`Kết quả mới cho ${key}_${index}: ${prize}`);
//                                     individualPromises.push(
//                                         publishToRedis(`${key}_${index}`, prize, formattedResult).then(() => {
//                                             lastPrizeData[key][index] = prize;
//                                         })
//                                     );
//                                 }
//                             }
//                         }
//                     } else if (!isArray && !completedPrizes[key]) {
//                         if (data && data !== '...') {
//                             const lastPrize = lastPrizeData[key] || '...';
//                             if (data !== lastPrize) {
//                                 console.log(`Kết quả mới cho ${key}: ${data}`);
//                                 individualPromises.push(
//                                     publishToRedis(key, data, formattedResult).then(() => {
//                                         lastPrizeData[key] = data;
//                                     })
//                                 );
//                             }
//                         }
//                     }
//                 }
//                 await Promise.all(individualPromises);

//                 // Cập nhật formattedResult từ lastPrizeData
//                 formattedResult.maDB = lastPrizeData.maDB;
//                 formattedResult.specialPrize = lastPrizeData.specialPrize;
//                 formattedResult.firstPrize = lastPrizeData.firstPrize;
//                 formattedResult.secondPrize = lastPrizeData.secondPrize;
//                 formattedResult.threePrizes = lastPrizeData.threePrizes;
//                 formattedResult.fourPrizes = lastPrizeData.fourPrizes;
//                 formattedResult.fivePrizes = lastPrizeData.fivePrizes;
//                 formattedResult.sixPrizes = lastPrizeData.sixPrizes;
//                 formattedResult.sevenPrizes = lastPrizeData.sevenPrizes;

//                 // Publish mảng từ lastPrizeData nếu đạt độ dài tối thiểu và khác với trạng thái trước
//                 const arrayPromises = [];
//                 for (const { key, isArray, minLength } of prizeTypes) {
//                     if (isArray && lastPrizeData[key].length >= minLength && lastPrizeData[key].every(prize => prize !== '...' && prize !== '****')) {
//                         if (JSON.stringify(lastPrizeData[key]) !== JSON.stringify(prevPrizeData[key])) {
//                             console.log(`Publish mảng ${key}:`, lastPrizeData[key]);
//                             arrayPromises.push(
//                                 publishToRedis(key, lastPrizeData[key], formattedResult).then(() => {
//                                     prevPrizeData[key] = lastPrizeData[key];
//                                 })
//                             );
//                         }
//                     }
//                 }
//                 await Promise.all(arrayPromises);

//                 if (
//                     formattedResult.maDB !== '...' ||
//                     formattedResult.specialPrize.some(prize => prize !== '...') ||
//                     formattedResult.firstPrize.some(prize => prize !== '...') ||
//                     formattedResult.secondPrize.some(prize => prize !== '...') ||
//                     formattedResult.threePrizes.some(prize => prize !== '...') ||
//                     formattedResult.fourPrizes.some(prize => prize !== '...') ||
//                     formattedResult.fivePrizes.some(prize => prize !== '...') ||
//                     formattedResult.sixPrizes.some(prize => prize !== '...') ||
//                     formattedResult.sevenPrizes.some(prize => prize !== '...')
//                 ) {
//                     await saveToMongoDB(formattedResult);
//                 } else {
//                     console.log(`Dữ liệu ngày ${date} cho ${station} chưa có, tiếp tục cào...`);
//                 }

//                 await logPerformance(iterationStart, iteration, true);
//                 successCount += 1;

//                 if (isDataComplete(formattedResult, completedPrizes)) {
//                     console.log(`Dữ liệu ngày ${date} cho ${station} đã đầy đủ, dừng cào.`);
//                     await logPerformance(iterationStart, iteration, true);
//                     isStopped = true;
//                     clearInterval(intervalId);
//                     const totalDuration = (Date.now() - startTime) / 1000;
//                     const stats = await pidusage(process.pid);
//                     console.log('Tổng hiệu suất scraper:', {
//                         totalDuration: `${totalDuration.toFixed(2)}s`,
//                         cpu: `${stats.cpu.toFixed(2)}%`,
//                         memory: `${(stats.memory / 1024 / 1024).toFixed(2)}MB`,
//                         totalIterations: iteration,
//                         successCount,
//                         errorCount,
//                     });
//                     if (page && !page.isClosed()) await page.close();
//                     if (browser) await browser.close();
//                     if (release) await release();
//                     return;
//                 }

//                 clearInterval(intervalId);
//                 intervalId = setInterval(scrapeAndSave, intervalMs);
//             } catch (error) {
//                 console.error(`Lỗi khi cào dữ liệu ngày ${date}:`, error.message);
//                 await logPerformance(iterationStart, iteration, false);
//                 errorCount += 1;
//             }
//         };

//         await scrapeAndSave(); // Chạy lần đầu ngay lập tức
//         intervalId = setInterval(scrapeAndSave, 3000);

//         setTimeout(async () => {
//             if (!isStopped) {
//                 isStopped = true;
//                 clearInterval(intervalId);
//                 console.log(`Dừng cào dữ liệu ngày ${date} sau 17 phút`);
//                 const totalDuration = (Date.now() - startTime) / 1000;
//                 const stats = await pidusage(process.pid);
//                 console.log('Tổng hiệu suất scraper:', {
//                     totalDuration: `${totalDuration.toFixed(2)}s`,
//                     cpu: `${stats.cpu.toFixed(2)}%`,
//                     memory: `${(stats.memory / 1024 / 1024).toFixed(2)}MB`,
//                     totalIterations: iteration,
//                     successCount,
//                     errorCount,
//                 });
//                 if (page && !page.isClosed()) await page.close();
//                 if (browser) await browser.close();
//                 if (release) await release();
//             }
//         }, 17 * 60 * 1000);

//     } catch (error) {
//         console.error(`Lỗi khi khởi động scraper ngày ${date}:`, error.message);
//         isStopped = true;
//         if (page && !page.isClosed()) await page.close();
//         if (browser) await browser.close();
//         if (release) await release();
//     }
// }

// module.exports = { scrapeXSMB };

// const [, , date, station] = process.argv;
// if (date && station) {
//     console.log(`Chạy thủ công cho ngày ${date} và đài ${station}`);
//     scrapeXSMB(date, station);
// } else {
//     console.log('Chạy thủ công: node scraper.js 29/04/2025 xsmb');
// }

// process.on('SIGINT', async () => {
//     await mongoose.connection.close();
//     await redisClient.quit();
//     console.log('Đã đóng kết nối MongoDB và Redis');
//     process.exit(0);
// });