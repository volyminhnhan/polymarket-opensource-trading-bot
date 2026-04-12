module.exports = {
    apps: [
        {
            name: 'redeem-resolved-service',
            cwd: __dirname,

            // Run via node with ts-node register (PM2 can't find ts-node in PATH)
            script: 'src/scripts/redeemResolvedPositions.ts',
            interpreter: 'node',
            node_args: '-r ts-node/register -r dotenv/config',
            args: '--loop',

            instances: 1,
            autorestart: true,

            // rerun every 60s AFTER the command exits
            restart_delay: 60000,

            // keep restarting forever (0 = unlimited in pm2)
            max_restarts: 0,

            env: { NODE_ENV: 'production' },

            // logs
            out_file: 'logs/redeem-resolved.out.log',
            error_file: 'logs/redeem-resolved.error.log',
            log_date_format: 'YYYY-MM-DD HH:mm:ss',
            merge_logs: true,
        },
    ],
};
