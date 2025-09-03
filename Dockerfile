# 🏗 **Stage 1: Build the Go Application**
FROM golang:1.19.13 AS builder

WORKDIR /optimusdbKB

# Install required libraries for SQLite & dqlite
RUN apt update && apt install -y \
    sqlite3 \
    libsqlite3-dev \
    libdqlite-dev \
    gcc \
    make \
    wget \
    curl

# Copy Go module files first (improves caching)
COPY go.mod go.sum ./
RUN go mod download

# Copy the entire project source code
COPY . .

# Before build
RUN go mod tidy
# Build the Go binary with CGO enabled for SQLite & dqlite support
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o optimusdb main.go

# 🏗 **Stage 2: Create a Minimal Runtime Image**
FROM ubuntu:22.04

WORKDIR /root/

# Install SQLite, dqlite, and additional utilities
RUN apt update && apt install -y \
    sqlite3 \
    libsqlite3-dev \
    libdqlite-dev \
    wget \
    curl \
    figlet \
    net-tools \
    iproute2 \
    htop \
    procps \
    vim \
    nano \
    fontconfig \
    fonts-dejavu \
    fonts-liberation \
    fonts-freefont-ttf \
    && fc-cache -fv

# ✅ Replaced obsolete `ttf-*` packages with `fonts-*` packages

# Environment Variables
ENV FONTCONFIG_PATH=/etc/fonts
ENV FONT_PATH=/usr/share/fonts
ENV FIGLET_FONTS_PATH=/usr/share/figlet
ENV LOKI_URL="http://192.168.2.99:3100/loki/api/v1/push"

# Expose application & dqlite ports
EXPOSE 8089 4001 5001 4242/udp 9001

# Copy the compiled Go binary from the builder stage
COPY --from=builder /optimusdbKB/optimusdb /root/optimusdb
RUN chmod +x /root/optimusdb

# Configure system network parameters
RUN echo "net.core.rmem_max=2097152" >> /etc/sysctl.conf && \
    echo "net.core.rmem_default=2097152" >> /etc/sysctl.conf

# Start the application
CMD ["/root/optimusdb"]