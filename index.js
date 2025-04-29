const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
const morgan = require('morgan');
const helmet = require('helmet');
const compression = require('compression');
const rateLimit = require('express-rate-limit');
require('dotenv').config();
const scraperRoutes = require('./src/routes/scraperRoutes');
const { startScraperScheduler } = require('./src/services/scraperScheduler');

const app = express();

// Middleware
app.use(cors());
app.use(compression());
app.use(morgan('dev'));
app.use(helmet());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Rate limiter
app.use('/api/scraper/scrape', rateLimit({
    windowMs: 60 * 1000,
    max: 10,
    message: 'Too many requests, please try again later',
}));

// API key middleware
app.use('/api/scraper/scrape', (req, res, next) => {
    const apiKey = req.headers['x-api-key'];
    if (apiKey !== process.env.API_KEY) {
        return res.status(401).json({ message: 'Invalid API key' });
    }
    next();
});

// MongoDB connection
mongoose.connect(process.env.MONGODB_URI, {
    maxPoolSize: 10,
    minPoolSize: 2,
    retryWrites: true,
}).then(() => console.log('Connected to MongoDB')).catch(err => console.error('MongoDB connection error:', err));

// Routes
app.use('/api/scraper', scraperRoutes);

// Health check
app.get('/api/scraper/health', (req, res) => {
    res.status(200).json({
        scraperRunning: !!global.intervalId,
        lastScrape: new Date().toISOString(),
        redisConnected: require('./src/scraper').redisClient?.isOpen || false,
        mongoConnected: mongoose.connection.readyState === 1,
    });
});

// Scheduler
const scraperConfig = {
    schedule: '15 11 * * *', // 18:15 VN = 11:15 UTC
    duration: 18 * 60 * 1000,
    station: 'xsmb',
};
startScraperScheduler(scraperConfig);

// Error handling
app.use((req, res) => {
    res.status(404).json({ message: 'Route not found' });
});
app.use((err, req, res, next) => {
    console.error(err.stack);
    res.status(500).json({ message: 'Internal server error' });
});

const PORT = process.env.PORT || 4000;
app.listen(PORT, () => {
    console.log(`Scraper API running on port ${PORT}`);
});