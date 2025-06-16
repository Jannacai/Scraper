"use strict"
const scaperMBRouter = require('./XSMB/scraperRoutes');
const scaperMnRouter = require('./XSMN/scraperRoutes');
const scaperMTRouter = require('./XSMT/scraperMTRoutes');
const Routes = (app) => {
    app.use('/api/scraperMT', scaperMTRouter);
    app.use('/api/scraperMN', scaperMnRouter);
    app.use('/api/scraper', scaperMBRouter);
};


module.exports = Routes;