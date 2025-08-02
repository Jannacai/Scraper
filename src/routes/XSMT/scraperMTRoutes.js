const express = require('express');
const { scrapeXSMT } = require('../../../scraperMT');
const scraperSchedulerMT = require('../../services/scraperSchedulerMT');
const router = express.Router();

console.log('scrapeXSMT:....', scrapeXSMT); // Debug require

// Endpoint để kích hoạt cào dữ liệu theo ngày
router.post('/scrapeMT', async (req, res) => {
    try {
        console.log('Received POST /api/scraperMT/xsmt/scrape:', req.body); // Debug request
        if (!req.body) {
            return res.status(400).json({ message: 'Request body is missing or invalid' });
        }
        const { date, station } = req.body;
        if (!date || !station) {
            return res.status(400).json({ message: 'Thiếu date hoặc station' });
        }
        console.log(`Bắt đầu cào dữ liệu cho ngày ${date}, đài ${station}`);
        scrapeXSMT(date, station); // Chạy bất đồng bộ
        res.status(200).json({ message: `Đã kích hoạt cào dữ liệu cho ngày ${date}, đài ${station}` });
    } catch (error) {
        console.error('Lỗi khi kích hoạt scraper:', error.message);
        res.status(500).json({ message: 'Lỗi khi kích hoạt scraper', error: error.message });
    }
});

// Endpoint để kiểm tra trạng thái scraper
router.get('/status', (req, res) => {
    res.status(200).json({ message: 'XSMT Scraper API is running' });
});

// Endpoint để kiểm tra trạng thái scheduler XSMT
router.get('/scheduler/status', (req, res) => {
    try {
        const status = scraperSchedulerMT.getStatus();
        res.status(200).json({
            success: true,
            data: status,
            message: 'Trạng thái scheduler XSMT'
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            message: 'Lỗi khi lấy trạng thái scheduler XSMT',
            error: error.message
        });
    }
});

// Endpoint để khởi động lại scheduler XSMT
router.post('/scheduler/restart', (req, res) => {
    try {
        scraperSchedulerMT.restart();
        res.status(200).json({
            success: true,
            message: 'Scheduler XSMT đã được khởi động lại'
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            message: 'Lỗi khi khởi động lại scheduler XSMT',
            error: error.message
        });
    }
});

// Endpoint để chạy scraper XSMT thủ công
router.post('/scheduler/run-now', async (req, res) => {
    try {
        const { date, station = 'xsmt' } = req.body;

        if (!date) {
            return res.status(400).json({
                success: false,
                message: 'Thiếu tham số date'
            });
        }

        console.log(`🎯 Chạy scraper XSMT thủ công cho ngày ${date}, đài ${station}`);

        // Chạy scraper trong background
        scraperSchedulerMT.runScraper().catch(error => {
            console.error('Lỗi khi chạy scraper XSMT thủ công:', error);
        });

        res.status(200).json({
            success: true,
            message: `Đã kích hoạt scraper XSMT cho ngày ${date}, đài ${station}`
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            message: 'Lỗi khi chạy scraper XSMT thủ công',
            error: error.message
        });
    }
});

module.exports = router;