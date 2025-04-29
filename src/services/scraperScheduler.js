const schedule = require('node-schedule');
const { scrapeXSMB } = require('../scraper');

const startScraperScheduler = (config) => {
    const { schedule: cronSchedule, duration, station } = config;
    console.log(`Starting scheduler with cron: ${cronSchedule}, duration: ${duration / 60000} minutes`);

    schedule.scheduleJob(cronSchedule, async () => {
        console.log('Starting XSMB scrape at 18:15...');
        const today = new Date().toLocaleDateString('vi-VN', {
            day: '2-digit',
            month: '2-digit',
            year: 'numeric',
        });
        const retryScrape = async (date, station, retries = 3) => {
            for (let i = 0; i < retries; i++) {
                try {
                    await scrapeXSMB(date, station);
                    return;
                } catch (error) {
                    console.warn(`Retry ${i + 1}: ${error.message}`);
                    if (i === retries - 1) throw error;
                    await new Promise(resolve => setTimeout(resolve, 1000 * Math.pow(2, i)));
                }
            }
        };
        await retryScrape(today, station);

        setTimeout(() => {
            console.log('Stopped XSMB scrape at 18:35.');
        }, duration);
    });
};

module.exports = { startScraperScheduler };