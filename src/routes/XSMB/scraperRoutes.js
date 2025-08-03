const express = require('express');
const { scrapeXSMB } = require('../../../scraper');
const scraperScheduler = require('../../services/scraperScheduler');
const router = express.Router();

console.log('scrapeXSMB:', scrapeXSMB); // Debug require

// Endpoint để kích hoạt cào dữ liệu theo ngày
router.post('/scrape', async (req, res) => {
    try {
        console.log('Received POST /api/scraper/scrape:', req.body); // Debug request
        if (!req.body) {
            return res.status(400).json({ message: 'Request body is missing or invalid' });
        }
        const { date, station } = req.body;
        if (!date || !station) {
            return res.status(400).json({ message: 'Thiếu date hoặc station' });
        }
        console.log(`Bắt đầu cào dữ liệu cho ngày ${date}, đài ${station}`);
        scrapeXSMB(date, station); // Chạy bất đồng bộ
        res.status(200).json({ message: `Đã kích hoạt cào dữ liệu cho ngày ${date}, đài ${station}` });
    } catch (error) {
        console.error('Lỗi khi kích hoạt scraper:', error.message);
        res.status(500).json({ message: 'Lỗi khi kích hoạt scraper', error: error.message });
    }
});

// Endpoint để kiểm tra trạng thái scraper
router.get('/status', (req, res) => {
    res.status(200).json({ message: 'Scraper API is running' });
});

// Endpoint để kiểm tra trạng thái scheduler
router.get('/scheduler/status', (req, res) => {
    try {
        const status = scraperScheduler.getStatus();
        res.status(200).json({
            message: 'Scheduler status retrieved successfully',
            data: status
        });
    } catch (error) {
        console.error('Lỗi khi lấy trạng thái scheduler:', error.message);
        res.status(500).json({
            message: 'Lỗi khi lấy trạng thái scheduler',
            error: error.message
        });
    }
});

// Endpoint để khởi động/dừng scheduler
router.post('/scheduler/control', (req, res) => {
    try {
        const { action } = req.body;

        if (!action || !['start', 'stop'].includes(action)) {
            return res.status(400).json({
                message: 'Action không hợp lệ. Chỉ chấp nhận: start, stop'
            });
        }

        if (action === 'start') {
            scraperScheduler.start();
            res.status(200).json({
                message: 'Scheduler đã được khởi động thành công'
            });
        } else if (action === 'stop') {
            scraperScheduler.stop();
            res.status(200).json({
                message: 'Scheduler đã được dừng thành công'
            });
        }
    } catch (error) {
        console.error('Lỗi khi điều khiển scheduler:', error.message);
        res.status(500).json({
            message: 'Lỗi khi điều khiển scheduler',
            error: error.message
        });
    }
});

// Endpoint để kích hoạt thủ công
router.post('/scheduler/trigger', async (req, res) => {
    try {
        const { date, station = 'xsmb' } = req.body;

        if (date && !/^\d{2}\/\d{2}\/\d{4}$/.test(date)) {
            return res.status(400).json({
                message: 'Format ngày không hợp lệ. Sử dụng DD/MM/YYYY'
            });
        }

        await scraperScheduler.manualTrigger(date, station);
        res.status(200).json({
            message: `Đã kích hoạt thủ công scraper cho ngày ${date || 'hôm nay'} - ${station}`
        });
    } catch (error) {
        console.error('Lỗi khi kích hoạt thủ công:', error.message);
        res.status(500).json({
            message: 'Lỗi khi kích hoạt thủ công',
            error: error.message
        });
    }
});

module.exports = router;