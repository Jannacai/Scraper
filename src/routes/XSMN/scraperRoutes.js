const express = require('express');
const { scrapeXSMN } = require('../../../scraperMN');
const router = express.Router();
const redis = require('redis');

const redisClient = redis.createClient({
    url: process.env.REDIS_URL || 'redis://localhost:6379',
});
redisClient.connect().catch(err => console.error('Lỗi kết nối Redis:', err));

console.log('scrapeXSMN:', scrapeXSMN);

router.post('/scrape', async (req, res) => {
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

        // Gọi scrapeXSMN với danh sách provinces
        await scrapeXSMN(date, station, provinces);

        res.status(200).json({ message: `Đã kích hoạt cào dữ liệu cho ngày ${date}, đài ${station}` });
    } catch (error) {
        console.error('Lỗi khi kích hoạt scraper:', error.message);
        res.status(500).json({ message: 'Lỗi khi kích hoạt scraper', error: error.message });
    }
});

router.get('/status', (req, res) => {
    res.status(200).json({ message: 'XSMN Scraper API is running' });
});

module.exports = router;