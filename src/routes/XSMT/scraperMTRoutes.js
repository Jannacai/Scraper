const express = require('express');
const { scrapeXSMT } = require('../../../scraperMT');
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

module.exports = router;