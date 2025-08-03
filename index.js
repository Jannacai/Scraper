const express = require('express');
const cors = require('cors');
const morgan = require('morgan');
const helmet = require('helmet');
const compression = require('compression');
const { connectMongoDB, closeMongoDB } = require('./db');
require('dotenv').config();
const routes = require('./src/routes/index');
const scraperScheduler = require('./src/services/scraperScheduler');
const scraperMTScheduler = require('./src/services/scraperMTScheduler');
const scraperMNScheduler = require('./src/services/scraperMNScheduler');
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
    
    // Khởi động scraper schedulers tự động
    setTimeout(() => {
        try {
            // Khởi động XSMB scheduler
            scraperScheduler.start();
            console.log('✅ XSMB Scraper Scheduler đã được khởi động tự động');
            
            // Khởi động XSMT scheduler
            scraperMTScheduler.start();
            console.log('✅ XSMT Scraper Scheduler đã được khởi động tự động');
            
            // Khởi động XSMN scheduler
            scraperMNScheduler.start();
            console.log('✅ XSMN Scraper Scheduler đã được khởi động tự động');
            
        } catch (error) {
            console.error('❌ Lỗi khi khởi động Scraper Schedulers:', error.message);
        }
    }, 2000); // Delay 2 giây để đảm bảo server đã sẵn sàng
});

// Đóng kết nối khi server dừng
process.on('SIGINT', async () => {
    console.log('🛑 Đang dừng server...');
    scraperScheduler.stop();
    scraperMTScheduler.stop();
    scraperMNScheduler.stop();
    await closeMongoDB();
    process.exit(0);
});

// Xử lý graceful shutdown
process.on('SIGTERM', async () => {
    console.log('🛑 Nhận tín hiệu SIGTERM, đang dừng server...');
    scraperScheduler.stop();
    scraperMTScheduler.stop();
    scraperMNScheduler.stop();
    await closeMongoDB();
    process.exit(0);
});