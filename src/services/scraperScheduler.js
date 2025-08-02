// const schedule = require('node-schedule');
// const { scrapeXSMB } = require('../../scraper');

// const startScraperScheduler = (config) => {
//     const { schedule: cronSchedule, duration, station } = config;
//     console.log(`Khởi động scheduler cào dữ liệu với lịch: ${cronSchedule}, thời gian chạy: ${duration / 60000} phút`);

//     schedule.scheduleJob(cronSchedule, () => {
//         console.log('Bắt đầu cào dữ liệu XSMB lúc 18:15...');
//         const today = new Date().toLocaleDateString('vi-VN', {
//             day: '2-digit',
//             month: '2-digit',
//             year: 'numeric',
//         });
//         scrapeXSMB(today, station);

//         // Dừng cào sau 20 phút (18:35)
//         setTimeout(() => {
//             console.log('Dừng cào dữ liệu XSMB lúc 18:35.');
//         }, duration);
//     });
// };

// module.exports = { startScraperScheduler };