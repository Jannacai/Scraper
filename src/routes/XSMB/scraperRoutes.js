const express = require('express');
const { scrapeXSMB } = require('../../../scraper');
const { scrapeXSMT } = require('../../../scraperMT');
const { scrapeXSMN } = require('../../../scraperMN');
const { testScraper, getSchedulerStatus, XSMB_CONFIG, XSMT_CONFIG, XSMN_CONFIG } = require('../../services/scraperScheduler');
const router = express.Router();

console.log('scrapeXSMB:', scrapeXSMB); // Debug require
console.log('scrapeXSMT:', scrapeXSMT); // Debug require
console.log('scrapeXSMN:', scrapeXSMN); // Debug require

// Endpoint để kích hoạt cào dữ liệu theo ngày (hỗ trợ cả XSMB và XSMT)
router.post('/scrape', async (req, res) => {
    try {
        console.log('Received POST /api/scraper/scrape:', req.body); // Debug request
        if (!req.body) {
            return res.status(400).json({ message: 'Request body is missing or invalid' });
        }
        const { date, station, type = 'xsmb', provinces } = req.body;
        if (!date || !station) {
            return res.status(400).json({ message: 'Thiếu date hoặc station' });
        }

        console.log(`Bắt đầu cào dữ liệu cho ngày ${date}, đài ${station}, loại ${type}`);

        // Chọn scraper function dựa trên type
        if (type === 'xsmt') {
            scrapeXSMT(date, station); // Chạy bất đồng bộ
        } else if (type === 'xsmn') {
            if (!provinces || !Array.isArray(provinces) || provinces.length === 0) {
                return res.status(400).json({ message: 'XSMN cần danh sách tỉnh' });
            }
            await scrapeXSMN(date, station, provinces); // Chạy đồng bộ cho XSMN
        } else {
            scrapeXSMB(date, station); // Chạy bất đồng bộ
        }

        res.status(200).json({
            message: `Đã kích hoạt cào dữ liệu cho ngày ${date}, đài ${station}, loại ${type.toUpperCase()}`
        });
    } catch (error) {
        console.error('Lỗi khi kích hoạt scraper:', error.message);
        res.status(500).json({ message: 'Lỗi khi kích hoạt scraper', error: error.message });
    }
});

// Endpoint để kích hoạt cào dữ liệu XSMB (backward compatibility)
router.post('/scrapeMB', async (req, res) => {
    try {
        console.log('Received POST /api/scraper/scrapeMB:', req.body);
        if (!req.body) {
            return res.status(400).json({ message: 'Request body is missing or invalid' });
        }
        const { date, station } = req.body;
        if (!date || !station) {
            return res.status(400).json({ message: 'Thiếu date hoặc station' });
        }
        console.log(`Bắt đầu cào dữ liệu XSMB cho ngày ${date}, đài ${station}`);
        scrapeXSMB(date, station);
        res.status(200).json({ message: `Đã kích hoạt cào dữ liệu XSMB cho ngày ${date}, đài ${station}` });
    } catch (error) {
        console.error('Lỗi khi kích hoạt scraper XSMB:', error.message);
        res.status(500).json({ message: 'Lỗi khi kích hoạt scraper XSMB', error: error.message });
    }
});

// Endpoint để kích hoạt cào dữ liệu XSMT (backward compatibility)
router.post('/scrapeMT', async (req, res) => {
    try {
        console.log('Received POST /api/scraper/scrapeMT:', req.body);
        if (!req.body) {
            return res.status(400).json({ message: 'Request body is missing or invalid' });
        }
        const { date, station } = req.body;
        if (!date || !station) {
            return res.status(400).json({ message: 'Thiếu date hoặc station' });
        }
        console.log(`Bắt đầu cào dữ liệu XSMT cho ngày ${date}, đài ${station}`);
        scrapeXSMT(date, station);
        res.status(200).json({ message: `Đã kích hoạt cào dữ liệu XSMT cho ngày ${date}, đài ${station}` });
    } catch (error) {
        console.error('Lỗi khi kích hoạt scraper XSMT:', error.message);
        res.status(500).json({ message: 'Lỗi khi kích hoạt scraper XSMT', error: error.message });
    }
});

// Endpoint để kích hoạt cào dữ liệu XSMN (backward compatibility)
router.post('/scrapeMN', async (req, res) => {
    try {
        console.log('Received POST /api/scraper/scrapeMN:', req.body);
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

        console.log(`Bắt đầu cào dữ liệu XSMN cho ngày ${date}, đài ${station}, tỉnh: ${provinces.map(p => p.tentinh).join(', ')}`);
        await scrapeXSMN(date, station, provinces);
        res.status(200).json({ message: `Đã kích hoạt cào dữ liệu XSMN cho ngày ${date}, đài ${station}` });
    } catch (error) {
        console.error('Lỗi khi kích hoạt scraper XSMN:', error.message);
        res.status(500).json({ message: 'Lỗi khi kích hoạt scraper XSMN', error: error.message });
    }
});

// Endpoint để kiểm tra trạng thái scraper
router.get('/status', (req, res) => {
    const status = getSchedulerStatus();
    res.status(200).json({
        message: 'Scraper API is running',
        schedulers: {
            xsmb: {
                status: 'active',
                schedule: XSMB_CONFIG.schedule,
                nextRun: status.xsmb.nextRun ? status.xsmb.nextRun.toISOString() : null,
                timezone: 'Asia/Ho_Chi_Minh',
                isRunning: status.xsmb.isRunning,
                lastRun: status.xsmb.lastRun ? status.xsmb.lastRun.toISOString() : null,
                totalRuns: status.xsmb.totalRuns,
                totalErrors: status.xsmb.totalErrors
            },
            xsmt: {
                status: 'active',
                schedule: XSMT_CONFIG.schedule,
                nextRun: status.xsmt.nextRun ? status.xsmt.nextRun.toISOString() : null,
                timezone: 'Asia/Ho_Chi_Minh',
                isRunning: status.xsmt.isRunning,
                lastRun: status.xsmt.lastRun ? status.xsmt.lastRun.toISOString() : null,
                totalRuns: status.xsmt.totalRuns,
                totalErrors: status.xsmt.totalErrors
            },
            xsmn: {
                status: 'active',
                schedule: XSMN_CONFIG.schedule,
                nextRun: status.xsmn.nextRun ? status.xsmn.nextRun.toISOString() : null,
                timezone: 'Asia/Ho_Chi_Minh',
                isRunning: status.xsmn.isRunning,
                lastRun: status.xsmn.lastRun ? status.xsmn.lastRun.toISOString() : null,
                totalRuns: status.xsmn.totalRuns,
                totalErrors: status.xsmn.totalErrors
            }
        }
    });
});

// Endpoint để test scraper ngay lập tức
router.post('/test', async (req, res) => {
    try {
        const { type = 'xsmb' } = req.body;
        console.log(`🧪 Test scraper được kích hoạt thủ công cho ${type.toUpperCase()}`);
        const success = await testScraper(type);

        if (success) {
            res.status(200).json({
                message: `Test scraper ${type.toUpperCase()} thành công`,
                timestamp: new Date().toISOString()
            });
        } else {
            res.status(500).json({
                message: `Test scraper ${type.toUpperCase()} thất bại`,
                timestamp: new Date().toISOString()
            });
        }
    } catch (error) {
        console.error('❌ Lỗi test scraper:', error);
        res.status(500).json({
            message: 'Lỗi khi test scraper',
            error: error.message,
            timestamp: new Date().toISOString()
        });
    }
});

// Endpoint để xem cấu hình scheduler
router.get('/config', (req, res) => {
    const status = getSchedulerStatus();
    res.status(200).json({
        schedulers: {
            xsmb: {
                config: XSMB_CONFIG,
                status: {
                    isRunning: status.xsmb.isRunning,
                    lastRun: status.xsmb.lastRun ? status.xsmb.lastRun.toISOString() : null,
                    nextRun: status.xsmb.nextRun ? status.xsmb.nextRun.toISOString() : null,
                    totalRuns: status.xsmb.totalRuns,
                    totalErrors: status.xsmb.totalErrors
                },
                description: {
                    schedule: 'Chạy lúc 18h14 mỗi ngày',
                    duration: '20 phút (từ 18h14 đến 18h34)',
                    retryAttempts: '3 lần thử nếu thất bại',
                    retryDelay: '5 giây giữa các lần thử'
                }
            },
            xsmt: {
                config: XSMT_CONFIG,
                status: {
                    isRunning: status.xsmt.isRunning,
                    lastRun: status.xsmt.lastRun ? status.xsmt.lastRun.toISOString() : null,
                    nextRun: status.xsmt.nextRun ? status.xsmt.nextRun.toISOString() : null,
                    totalRuns: status.xsmt.totalRuns,
                    totalErrors: status.xsmt.totalErrors
                },
                description: {
                    schedule: 'Chạy lúc 17h14 mỗi ngày',
                    duration: '20 phút (từ 17h14 đến 17h34)',
                    retryAttempts: '3 lần thử nếu thất bại',
                    retryDelay: '5 giây giữa các lần thử'
                }
            },
            xsmn: {
                config: XSMN_CONFIG,
                status: {
                    isRunning: status.xsmn.isRunning,
                    lastRun: status.xsmn.lastRun ? status.xsmn.lastRun.toISOString() : null,
                    nextRun: status.xsmn.nextRun ? status.xsmn.nextRun.toISOString() : null,
                    totalRuns: status.xsmn.totalRuns,
                    totalErrors: status.xsmn.totalErrors
                },
                description: {
                    schedule: 'Chạy lúc 16h12 mỗi ngày',
                    duration: '30 phút (từ 16h12 đến 16h42)',
                    retryAttempts: '3 lần thử nếu thất bại',
                    retryDelay: '5 giây giữa các lần thử',
                    automation: 'Tự động hóa hoàn toàn'
                }
            }
        }
    });
});

// Endpoint để xem logs chi tiết
router.get('/logs', (req, res) => {
    const status = getSchedulerStatus();
    res.status(200).json({
        uptime: process.uptime(),
        memory: process.memoryUsage(),
        schedulers: status,
        environment: {
            nodeEnv: process.env.NODE_ENV,
            timezone: process.env.TIMEZONE || 'Asia/Ho_Chi_Minh',
            port: process.env.PORT || 4000
        }
    });
});

module.exports = router;