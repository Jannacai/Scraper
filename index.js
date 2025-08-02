const express = require('express');
const cors = require('cors');
const morgan = require('morgan');
const helmet = require('helmet');
const compression = require('compression');
const { connectMongoDB, closeMongoDB } = require('./db');
require('dotenv').config();
const routes = require('./src/routes/index');
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
});

// Đóng kết nối khi server dừng
process.on('SIGINT', async () => {
    await closeMongoDB();
    process.exit(0);
});