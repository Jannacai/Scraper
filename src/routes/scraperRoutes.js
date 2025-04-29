const express = require('express');
const router = express.Router();
const { scrapeXSMB } = require('../scraper');

router.post('/scrape', async (req, res) => {
    try {
        const { date, station } = req.body;
        if (!date || !station) {
            return res.status(400).json({ message: 'Missing date or station' });
        }
        await scrapeXSMB(date, station);
        res.status(200).json({ message: `Started scraping for ${date} (${station})` });
    } catch (error) {
        console.error('Scrape error:', error);
        res.status(500).json({ message: 'Failed to start scraping', error: error.message });
    }
});

module.exports = router;