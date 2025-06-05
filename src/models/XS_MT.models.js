const mongoose = require('mongoose');

const xsmtSchema = new mongoose.Schema({
    drawDate: { type: Date, required: true },
    dayOfWeek: { type: String },
    tentinh: { type: String, required: true },
    tinh: { type: String, required: true },
    slug: { type: String, unique: true },
    year: { type: Number },
    month: { type: Number },
    eightPrizes: { type: [String] },
    sevenPrizes: { type: [String] },
    sixPrizes: { type: [String] },
    fivePrizes: { type: [String] },
    fourPrizes: { type: [String] },
    threePrizes: { type: [String] },
    secondPrize: { type: [String] },
    firstPrize: { type: [String] },
    specialPrize: { type: [String] },
    station: { type: String, required: true },
    createdAt: { type: Date, default: Date.now },
}, {
    indexes: [
        { key: { drawDate: 1, station: 1, tentinh: 1 }, unique: true },
        { key: { drawDate: -1, station: 1 } },
        { key: { slug: 1 }, unique: true },
        { key: { tentinh: 1 } },
        { key: { dayOfWeek: 1 } },
        { key: { station: 1, tinh: 1 } },
    ],
});

xsmtSchema.pre('save', function (next) {
    this.updatedAt = Date.now();
    next();
});

module.exports = mongoose.model('XSMT', xsmtSchema);