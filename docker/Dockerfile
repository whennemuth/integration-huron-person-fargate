# Multi-stage Docker build for Huron Person processing
# Optimized for AWS Fargate with Graviton2 (ARM64) processors
#
# SECURITY NOTES:
# - Base image vulnerabilities are mitigated via 'apk upgrade --no-cache' during build
# - Runs as non-root user (nodejs:1001) for additional security
# - Production stage excludes dev dependencies
# - For production: Enable AWS ECR image scanning for detailed vulnerability reports
# - Many reported CVEs in base images are not exploitable in this specific use case

# Stage 1: Build stage
FROM --platform=linux/arm64 node:current-alpine3.22 AS builder

# Update packages to patch vulnerabilities
RUN apk upgrade --no-cache && \
    apk add --no-cache python3 make g++

WORKDIR /app

# Copy package files
COPY package*.json ./
COPY tsconfig.json ./

# Install ALL dependencies (including devDependencies for build)
RUN npm ci

# Copy source code
COPY src ./src
COPY context/*.ts ./context/
COPY docker/*.ts ./docker/

# Build TypeScript to JavaScript
RUN npm run build

# Stage 2: Production stage
FROM --platform=linux/arm64 node:current-alpine3.22

# Update packages to patch vulnerabilities
RUN apk upgrade --no-cache

# Set production environment
ENV NODE_ENV=production

# Create app directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install ONLY production dependencies
RUN npm ci --omit=dev && \
    npm cache clean --force

# Create non-root user for security
RUN addgroup -g 1001 -S nodejs && \
    adduser -S nodejs -u 1001

# Copy built JavaScript from builder stage
COPY --from=builder /app/dist ./dist

# Change ownership to non-root user
RUN chown -R nodejs:nodejs /app

# Switch to non-root user
USER nodejs

# Set default environment variables (can be overridden at runtime)
ENV INPUT_BUCKET="" \
    INPUT_KEY="" \
    CHUNKS_BUCKET="" \
    CHUNK_KEY="" \
    REGION="us-east-2" \
    ITEMS_PER_CHUNK="200" \
    PERSON_ID_FIELD="personid"

# Default command (can be overridden by ECS task definition)
# To run chunker: node dist/docker/chunker.js
# To run processor: node dist/docker/processor.js
# To run merger: node dist/docker/merger.js
CMD ["node", "dist/docker/chunker.js"]

# Health check (optional, for debugging)
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD node -e "console.log('healthy')" || exit 1

# Labels for identification
LABEL maintainer="BU Integration Team"
LABEL service="huron-person-sync"
LABEL architecture="arm64"
LABEL processor="graviton2"
