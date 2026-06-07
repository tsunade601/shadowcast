FROM node:20-alpine

WORKDIR /app

# Copy package files first for better caching
COPY library/package.json library/tsconfig.json library/
COPY demo/package.json demo/

# Install dependencies for both
RUN cd library && npm install
RUN cd demo && npm install

# Copy source code
COPY library/ library/
COPY demo/ demo/

# Build library first (demo depends on it)
RUN cd library && npm run build

# NEXT_PUBLIC_* vars are inlined at build time, so pass as ARG
ARG NEXT_PUBLIC_SIGNALING_URL=ws://localhost:8080
ENV NEXT_PUBLIC_SIGNALING_URL=$NEXT_PUBLIC_SIGNALING_URL

# Build demo
WORKDIR /app/demo
RUN npm run build

EXPOSE 3000

CMD ["npm", "start"]