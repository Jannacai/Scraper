const mongoose = require('mongoose');

const xsmbSchema = new mongoose.Schema({
    drawDate: { type: Date, required: true },
    dayOfWeek: { type: String },
    tentinh: { type: String, required: true },
    tinh: { type: String, required: true },
    slug: { type: String, unique: true },
    year: { type: Number },
    month: { type: Number },
    maDB: { type: String },
    specialPrize: { type: [String] },
    firstPrize: { type: [String] },
    secondPrize: { type: [String] },
    threePrizes: { type: [String] },
    fourPrizes: { type: [String] },
    fivePrizes: { type: [String] },
    sixPrizes: { type: [String] },
    sevenPrizes: { type: [String] },
    station: { type: String, required: true },
    createdAt: { type: Date, default: Date.now },
}, {
    indexes: [
        { key: { drawDate: 1, station: 1 }, unique: true },
        { key: { drawDate: -1, station: 1 } },
        { key: { slug: 1 }, unique: true },
        { key: { dayOfWeek: 1 } },
        { key: { station: 1, tinh: 1 } },
    ],
});

xsmbSchema.pre('save', function (next) {
    this.updatedAt = Date.now();
    next();
});

module.exports = mongoose.model('XSMB', xsmbSchema);