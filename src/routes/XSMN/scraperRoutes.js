const express = require('express');
const { scrapeXSMN } = require('../../../scraperMN');
const scraperMNScheduler = require('../../services/scraperMNScheduler');
const router = express.Router();
const redis = require('redis');

const redisClient = redis.createClient({
    url: process.env.REDIS_URL || 'redis://localhost:6379',
});
redisClient.connect().catch(err => console.error('Lỗi kết nối Redis:', err));

console.log('scrapeXSMN:', scrapeXSMN);

router.post('/scrapeMN', async (req, res) => {
    try {
        console.log('Received POST /api/scraperMN/scrape:', req.body);
        if (!req.body) {
            return res.status(400).json({ message: 'Request body is missing or invalid' });
        }
        const { date, station, provinces } = req.body;
        if (!date || !station) {
            return res.status(400).json({ message: 'Thiếu date hoặc station' });
        }
        if (!provinces || !Array.isArray(provinces) || provinces.length === 0) {
            return res.status(400).json({ message: 'Thiếu danh sách tỉnh hoặc danh sách tỉnh không hợp lệ' });
        }

        console.log(`Bắt đầu cào dữ liệu cho ngày ${date}, đài ${station}, tỉnh: ${provinces.map(p => p.tentinh).join(', ')}`);

        // Gọi scrapeXSMN với isTestMode = false
        await scrapeXSMN(date, station, false);

        res.status(200).json({ message: `Đã kích hoạt cào dữ liệu cho ngày ${date}, đài ${station}` });
    } catch (error) {
        console.error('Lỗi khi kích hoạt scraper:', error.message);
        res.status(500).json({ message: 'Lỗi khi kích hoạt scraper', error: error.message });
    }
});

router.get('/status', (req, res) => {
    res.status(200).json({ message: 'XSMN Scraper API is running' });
});

// ✅ MỚI: Endpoint để kiểm tra trạng thái scheduler
router.get('/scheduler/status', (req, res) => {
    try {
        const status = scraperMNScheduler.getStatus();
        res.status(200).json({
            message: 'XSMN Scheduler status retrieved successfully',
            data: status
        });
    } catch (error) {
        console.error('Lỗi khi lấy trạng thái XSMN scheduler:', error.message);
        res.status(500).json({
            message: 'Lỗi khi lấy trạng thái XSMN scheduler',
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
            scraperMNScheduler.start();
            res.status(200).json({
                message: 'XSMN Scheduler đã được khởi động thành công'
            });
        } else if (action === 'stop') {
            scraperMNScheduler.stop();
            res.status(200).json({
                message: 'XSMN Scheduler đã được dừng thành công'
            });
        }
    } catch (error) {
        console.error('Lỗi khi điều khiển XSMN scheduler:', error.message);
        res.status(500).json({
            message: 'Lỗi khi điều khiển XSMN scheduler',
            error: error.message
        });
    }
});

// ✅ MỚI: Endpoint để kích hoạt thủ công
router.post('/scheduler/trigger', async (req, res) => {
    try {
        const { date, station = 'xsmn', provinces = null } = req.body;

        if (date && !/^\d{2}\/\d{2}\/\d{4}$/.test(date)) {
            return res.status(400).json({
                message: 'Format ngày không hợp lệ. Sử dụng DD/MM/YYYY'
            });
        }

        await scraperMNScheduler.manualTrigger(date, station, provinces);
        res.status(200).json({
            message: `Đã kích hoạt thủ công scraper XSMN cho ngày ${date || 'hôm nay'} - ${station}${provinces ? ` - ${provinces.length} tỉnh` : ''}`
        });
    } catch (error) {
        console.error('Lỗi khi kích hoạt thủ công XSMN:', error.message);
        res.status(500).json({
            message: 'Lỗi khi kích hoạt thủ công XSMN',
            error: error.message
        });
    }
});

module.exports = router;