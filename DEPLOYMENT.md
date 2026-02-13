# Deployment Guide - Digital Ocean with Docker

This guide explains how to deploy the Metabase Dashboard Service to Digital Ocean using Docker.

## Prerequisites

- Digital Ocean Droplet (Ubuntu 20.04 or later recommended)
- MongoDB instance (MongoDB Atlas or self-hosted)
- Domain name (optional, for SSL)

## Quick Deployment Steps

### 1. SSH into Your Digital Ocean Server

```bash
ssh root@your_server_ip
```

### 2. Install Docker and Docker Compose

```bash
# Update packages
apt update && apt upgrade -y

# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sh get-docker.sh

# Install Docker Compose
apt install docker-compose -y

# Verify installation
docker --version
docker-compose --version
```

### 3. Clone the Repository

```bash
# Install git
apt install git -y

# Clone the deployment branch
cd /opt
git clone -b deployment https://github.com/YOUR_USERNAME/YOUR_REPO.git metabase-dashboard
cd metabase-dashboard
```

### 4. Configure Environment Variables

Create a `.env` file with your credentials:

```bash
nano .env
```

Add the following (replace with your actual values):

```env
# MongoDB Connection (REQUIRED)
MONGODB_URI=mongodb+srv://username:password@cluster.mongodb.net/?retryWrites=true&w=majority

# Microsoft OAuth Authentication
MICROSOFT_CLIENT_ID=your_client_id
MICROSOFT_CLIENT_SECRET=your_client_secret
MICROSOFT_TENANT_ID=your_tenant_id
ALLOWED_EMAIL_DOMAIN=yourdomain.com
SESSION_SECRET_KEY=generate-a-secure-random-string-here

# Optional: Metabase credentials (can also be set via web UI)
METABASE_URL=https://your-metabase-instance.com
METABASE_USERNAME=your_username
METABASE_PASSWORD=your_password
```

Save and exit (Ctrl+X, then Y, then Enter).

### 5. Build and Start the Container

```bash
docker-compose up -d --build
```

### 6. Verify the Deployment

```bash
# Check container status
docker-compose ps

# View logs
docker-compose logs -f

# Test the service
curl http://localhost:1206
```

### 7. Configure Firewall

```bash
# Allow necessary ports
ufw allow OpenSSH
ufw allow 1206/tcp
ufw enable
```

## Access Your Application

- Direct access: `http://your_server_ip:1206`
- Configure via web UI: Navigate to the URL and click "Settings"

## Optional: Set Up Nginx Reverse Proxy with SSL

### Install Nginx

```bash
apt install nginx -y
```

### Create Nginx Configuration

```bash
nano /etc/nginx/sites-available/metabase-dashboard
```

Add this configuration:

```nginx
server {
    listen 80;
    server_name your_domain.com;

    location / {
        proxy_pass http://localhost:1206;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_cache_bypass $http_upgrade;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

### Enable the Site

```bash
ln -s /etc/nginx/sites-available/metabase-dashboard /etc/nginx/sites-enabled/
nginx -t
systemctl reload nginx

# Update firewall
ufw allow 'Nginx Full'
```

### Install SSL Certificate

```bash
# Install Certbot
apt install certbot python3-certbot-nginx -y

# Get SSL certificate (replace with your domain)
certbot --nginx -d your_domain.com

# Auto-renewal is configured automatically
```

## Useful Docker Commands

```bash
# View logs
docker-compose logs -f

# Restart the service
docker-compose restart

# Stop the service
docker-compose down

# Rebuild after code changes
git pull origin deployment
docker-compose up -d --build

# Execute commands inside container
docker-compose exec metabase-dashboard-service bash

# View running containers
docker ps
```

## Updating the Application

```bash
cd /opt/metabase-dashboard
git pull origin deployment
docker-compose up -d --build
```

## Troubleshooting

### Check Container Status
```bash
docker-compose ps
```

### View Logs
```bash
docker-compose logs -f metabase-dashboard-service
```

### Check MongoDB Connection
Navigate to `http://your_server_ip:1206/api/mongodb-status`

### Restart Container
```bash
docker-compose restart
```

### Container Won't Start
```bash
# Check logs for errors
docker-compose logs

# Verify .env file exists and has correct values
cat .env

# Check if port 1206 is available
netstat -tulpn | grep 1206
```

## Important Security Notes

1. **Never commit `.env` file** - It contains sensitive credentials
2. **Use strong passwords** - Generate a secure `SESSION_SECRET_KEY`
3. **Update Microsoft OAuth redirect URIs** - Add your production URL to Azure app registration
4. **Keep server updated** - Run `apt update && apt upgrade` regularly
5. **Configure firewall** - Only open necessary ports
6. **Use SSL in production** - Follow the Nginx + Let's Encrypt setup above

## Monitoring

Consider setting up monitoring:
- Container health checks
- Log aggregation (e.g., ELK stack)
- Uptime monitoring (e.g., UptimeRobot)
- Resource monitoring (e.g., Netdata, Prometheus)

## Support

For issues or questions, check the main README.md or application logs.
