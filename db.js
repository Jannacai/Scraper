const mongoose = require('mongoose');
require('dotenv').config();

let mongooseConnected = false;

async function connectMongoDB() {
    if (mongooseConnected || mongoose.connection.readyState === 1) {
        console.log('MongoDB already connected');
        return;
    }
    try {
        await mongoose.connect(process.env.MONGODB_URI || 'mongodb://localhost:27017/xsmb', {
            maxPoolSize: 10,
            minPoolSize: 2,
        });
        mongooseConnected = true;
        console.log('Đã kết nối MongoDB');
    } catch (err) {
        console.error('Lỗi kết nối MongoDB:', err.message);
        throw err;
    }
}

function isConnected() {
    return mongooseConnected && mongoose.connection.readyState === 1;
}

async function closeMongoDB() {
    if (mongooseConnected && mongoose.connection.readyState === 1) {
        await mongoose.connection.close();
        mongooseConnected = false;
        console.log('Đã đóng kết nối MongoDB');
    }
}

module.exports = { connectMongoDB, isConnected, closeMongoDB };