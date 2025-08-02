"use strict"
const scaperMBRouter = require('./XSMB/scraperRoutes');
const Routes = (app) => {
    app.use('/api/scraper', scaperMBRouter);
    // XSMN đã được gộp vào /api/scraper với endpoint /scrapeMN
};

module.exports = Routes;