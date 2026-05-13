FROM node:20-alpine

WORKDIR /app

# Install deps first for layer caching
COPY package.json ./
RUN npm install --omit=dev --no-audit --no-fund

COPY server.js ./

ENV NODE_ENV=production
ENV PORT=3001
EXPOSE 3001

# Drop privileges
USER node

CMD ["node", "server.js"]
