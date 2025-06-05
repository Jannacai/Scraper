FROM node:18-slim

# Cài đặt các thư viện phụ thuộc cho Puppeteer
RUN apt-get update && apt-get install -y \
    libglib2.0-0 \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpango-1.0-0 \
    libcairo2 \
    fonts-liberation \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Thiết lập thư mục làm việc
WORKDIR /app

# Sao chép và cài đặt dependencies
COPY package*.json ./
RUN npm install

# Sao chép mã nguồn
COPY . .

# Cấu hình môi trường
ENV NODE_ENV=production
EXPOSE 4000

# Chạy ứng dụng
CMD ["node", "index.js"]