const express = require('express');
const cors = require('cors');
const morgan = require('morgan');
const helmet = require('helmet');
const compression = require('compression');
const { connectMongoDB, closeMongoDB } = require('./db');
require('dotenv').config();
const routes = require('./src/routes/index');

// Import scraper scheduler
const scraperScheduler = require('./src/services/scraperScheduler');

const app = express();

// Middleware
app.use(cors());
app.use(compression());
app.use(morgan('dev'));
app.use(helmet());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Kết nối MongoDB
connectMongoDB().catch(err => {
    console.error('MongoDB connection error:', err);
    process.exit(1);
});

routes(app);

// Thêm endpoint để kiểm tra trạng thái scheduler
app.get('/api/scheduler/status', (req, res) => {
    try {
        const status = scraperScheduler.getStatus();
        res.json({
            success: true,
            data: status,
            message: 'Trạng thái scheduler'
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            message: 'Lỗi khi lấy trạng thái scheduler',
            error: error.message
        });
    }
});

// Thêm endpoint để khởi động lại scheduler
app.post('/api/scheduler/restart', (req, res) => {
    try {
        scraperScheduler.restart();
        res.json({
            success: true,
            message: 'Scheduler đã được khởi động lại'
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            message: 'Lỗi khi khởi động lại scheduler',
            error: error.message
        });
    }
});

// Thêm endpoint để chạy scraper thủ công
app.post('/api/scheduler/run-now', async (req, res) => {
    try {
        const { date, station = 'xsmb' } = req.body;
        
        if (!date) {
            return res.status(400).json({
                success: false,
                message: 'Thiếu tham số date'
            });
        }

        console.log(`🎯 Chạy scraper thủ công cho ngày ${date}, đài ${station}`);
        
        // Chạy scraper trong background
        scraperScheduler.runScraper().catch(error => {
            console.error('Lỗi khi chạy scraper thủ công:', error);
        });

        res.json({
            success: true,
            message: `Đã kích hoạt scraper cho ngày ${date}, đài ${station}`
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            message: 'Lỗi khi chạy scraper thủ công',
            error: error.message
        });
    }
});

app.use((req, res) => {
    res.status(404).json({ message: 'Route not found' });
});

// Xử lý lỗi server
app.use((err, req, res, next) => {
    console.error(err.stack);
    res.status(500).json({ message: 'Internal server error' });
});

const PORT = process.env.PORT || 4000;
app.listen(PORT, () => {
    console.log(`Scraper API running on port ${PORT}`);
    console.log('🚀 Scraper Scheduler đã được khởi động tự động');
});

// Đóng kết nối khi server dừng
process.on('SIGINT', async () => {
    console.log('🛑 Đang dừng server...');
    await closeMongoDB();
    scraperScheduler.stop();
    process.exit(0);
});

process.on('SIGTERM', async () => {
    console.log('🛑 Đang dừng server...');
    await closeMongoDB();
    scraperScheduler.stop();
    process.exit(0);
});