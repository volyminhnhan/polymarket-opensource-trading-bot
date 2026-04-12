import * as dotenv from 'dotenv';
import * as path from 'path';

// Load .env from parent polymarket_konis directory (shared with V4 bot)
dotenv.config({ path: path.resolve(__dirname, '..', '..', '..', '.env') });

// Minimal ENV for redeem-resolved service
// Only validates what redeemResolvedPositions.ts actually uses

const PROXY_WALLET = process.env.PROXY_WALLET || process.env.POLYMARKET_FUNDER;
const PRIVATE_KEY = process.env.PRIVATE_KEY;
const RPC_URL = process.env.RPC_URL || process.env.POLYGON_RPC_URL || 'https://polygon-rpc.com';

if (!PRIVATE_KEY) {
    throw new Error('Missing PRIVATE_KEY in .env');
}
if (!PROXY_WALLET) {
    throw new Error('Missing PROXY_WALLET or POLYMARKET_FUNDER in .env');
}

export const ENV = {
    PROXY_WALLET,
    PRIVATE_KEY,
    RPC_URL,
    NETWORK_RETRY_LIMIT: parseInt(process.env.NETWORK_RETRY_LIMIT || '3', 10),
    REQUEST_TIMEOUT_MS: parseInt(process.env.REQUEST_TIMEOUT_MS || '10000', 10),
};
