# FHCharts DigitalOcean Worker

Real-time cryptocurrency market data processing worker for DigitalOcean App Platform.

## Features
- Real-time WebSocket connection to Binance
- PostgreSQL database integration
- Automatic reconnection and error handling
- Volume profile and order flow analysis
- Multiple timeframe support

## Deployment

1. Create a new app on DigitalOcean App Platform
2. Connect your GitHub repository
3. Use the provided `app.yaml` configuration
4. Set environment variables:
   - `DATABASE_URL` (auto-provided by DigitalOcean)
   - `SYMBOL` (e.g., 'xmrusdt')

## Environment Variables
- `DATABASE_URL`: PostgreSQL connection string (auto-provided)
- `SYMBOL`: Trading pair symbol (default: xmrusdt)

## Database Schema
The worker creates a `timeframe_data` table with columns for:
- Basic OHLCV data
- Volume profiles
- Order flow metrics
- Market microstructure data

## Monitoring
Check application logs in DigitalOcean dashboard for:
- Connection status
- Data processing metrics
- Error handling
