const express = require('express');
const { scrapeXSMT } = require('../../../scraperMT');
const scraperMTScheduler = require('../../services/scraperMTScheduler');
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

// ✅ MỚI: Endpoint để kiểm tra trạng thái scheduler
router.get('/scheduler/status', (req, res) => {
    try {
        const status = scraperMTScheduler.getStatus();
        res.status(200).json({
            message: 'XSMT Scheduler status retrieved successfully',
            data: status
        });
    } catch (error) {
        console.error('Lỗi khi lấy trạng thái XSMT scheduler:', error.message);
        res.status(500).json({
            message: 'Lỗi khi lấy trạng thái XSMT scheduler',
            error: error.message
        });
    }
});

// ✅ MỚI: Endpoint để điều khiển scheduler
router.post('/scheduler/control', (req, res) => {
    try {
        const { action } = req.body;

        if (!action || !['start', 'stop'].includes(action)) {
            return res.status(400).json({
                message: 'Action không hợp lệ. Chỉ chấp nhận: start, stop'
            });
        }

        if (action === 'start') {
            scraperMTScheduler.start();
            res.status(200).json({
                message: 'XSMT Scheduler đã được khởi động thành công'
            });
        } else if (action === 'stop') {
            scraperMTScheduler.stop();
            res.status(200).json({
                message: 'XSMT Scheduler đã được dừng thành công'
            });
        }
    } catch (error) {
        console.error('Lỗi khi điều khiển XSMT scheduler:', error.message);
        res.status(500).json({
            message: 'Lỗi khi điều khiển XSMT scheduler',
            error: error.message
        });
    }
});

// ✅ MỚI: Endpoint để kích hoạt thủ công
router.post('/scheduler/trigger', async (req, res) => {
    try {
        const { date, station = 'xsmt', province = null } = req.body;

        if (date && !/^\d{2}\/\d{2}\/\d{4}$/.test(date)) {
            return res.status(400).json({
                message: 'Format ngày không hợp lệ. Sử dụng DD/MM/YYYY'
            });
        }

        await scraperMTScheduler.manualTrigger(date, station, province);
        res.status(200).json({
            message: `Đã kích hoạt thủ công scraper XSMT cho ngày ${date || 'hôm nay'} - ${station}${province ? ` - ${province}` : ''}`
        });
    } catch (error) {
        console.error('Lỗi khi kích hoạt thủ công XSMT:', error.message);
        res.status(500).json({
            message: 'Lỗi khi kích hoạt thủ công XSMT',
            error: error.message
        });
    }
});

module.exports = router;