import { ethers } from 'ethers';
import { ENV } from '../config/env';
import fetchData from '../utils/fetchData';

const PROXY_WALLET = ENV.PROXY_WALLET;
const PRIVATE_KEY = ENV.PRIVATE_KEY;
const RPC_URL = ENV.RPC_URL || 'https://polygon-rpc.com';

// Contract addresses on Polygon
const CTF_CONTRACT_ADDRESS = '0x4D97DCd97eC945f40cF65F87097ACe5EA0476045';
const NEG_RISK_CTF_ADDRESS = '0xC5d563A36AE78145C45a50134d48A1215220f80a';
const USDC_ADDRESS = '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174';

const RESOLVED_HIGH = 0.99;
const RESOLVED_LOW = 0.01;
const ZERO_THRESHOLD = 0.0001;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const RPC_DELAY = 10000; // 10s between RPC calls — RPC rate limit says "retry in 10s"

interface Position {
    asset: string;
    conditionId: string;
    size: number;
    avgPrice: number;
    currentValue: number;
    curPrice: number;
    title?: string;
    outcome?: string;
    slug?: string;
    redeemable?: boolean;
    negativeRisk?: boolean;
}

const CTF_ABI = [
    'function redeemPositions(address collateralToken, bytes32 parentCollectionId, bytes32 conditionId, uint256[] calldata indexSets) external',
    'function balanceOf(address owner, uint256 tokenId) external view returns (uint256)',
];

// Gnosis Safe ABI for execTransaction
const GNOSIS_SAFE_ABI = [
    'function execTransaction(address to, uint256 value, bytes data, uint8 operation, uint256 safeTxGas, uint256 baseGas, uint256 gasPrice, address gasToken, address refundReceiver, bytes signatures) external payable returns (bool success)',
    'function nonce() view returns (uint256)',
    'function getOwners() view returns (address[])',
];

// Check if address is a contract (Gnosis Safe proxy)
const isGnosisSafe = async (address: string, provider: ethers.providers.JsonRpcProvider): Promise<boolean> => {
    const code = await provider.getCode(address);
    return code !== '0x';
};

// Build pre-approved owner signature for Gnosis Safe (threshold=1)
// r=ownerAddress padded to 32 bytes, s=0, v=1
const buildOwnerSignature = (ownerAddress: string): string => {
    const r = ethers.utils.hexZeroPad(ownerAddress, 32);
    const s = ethers.utils.hexZeroPad('0x0', 32);
    const v = '01';
    return r + s.slice(2) + v;
};

// Encode redeemPositions calldata for the CTF contract
const encodeRedeemCalldata = (conditionId: string, ctfAddress: string): string => {
    const ctfInterface = new ethers.utils.Interface(CTF_ABI);
    let cid = conditionId.startsWith('0x') ? conditionId : '0x' + conditionId;
    return ctfInterface.encodeFunctionData('redeemPositions', [
        USDC_ADDRESS,
        ethers.constants.HashZero,
        ethers.utils.hexZeroPad(cid, 32),
        [1, 2],
    ]);
};

const loadPositions = async (address: string): Promise<Position[]> => {
    const url = `https://data-api.polymarket.com/positions?user=${address}`;
    const data = await fetchData(url);
    const positions = Array.isArray(data) ? (data as Position[]) : [];
    return positions.filter((pos) => (pos.size || 0) > ZERO_THRESHOLD);
};

// Redeem via Gnosis Safe execTransaction - the Safe calls redeemPositions as msg.sender
const redeemViaGnosisSafe = async (
    wallet: ethers.Wallet,
    proxyAddress: string,
    position: Position,
    useNegRisk: boolean = false
): Promise<{ success: boolean; txHash?: string; error?: string }> => {
    const ctfAddress = useNegRisk ? NEG_RISK_CTF_ADDRESS : CTF_CONTRACT_ADDRESS;

    try {
        console.log(`   Attempting redemption via Gnosis Safe on ${useNegRisk ? 'NEG_RISK' : 'CTF'} contract...`);

        const safeContract = new ethers.Contract(proxyAddress, GNOSIS_SAFE_ABI, wallet);
        const calldata = encodeRedeemCalldata(position.conditionId, ctfAddress);
        const signature = buildOwnerSignature(wallet.address);

        await sleep(RPC_DELAY);
        const feeData = await wallet.provider!.getFeeData();
        const gasPrice = feeData.gasPrice || feeData.maxFeePerGas;
        const adjustedGasPrice = gasPrice ? gasPrice.mul(120).div(100) : undefined;

        await sleep(RPC_DELAY);
        const tx = await safeContract.execTransaction(
            ctfAddress, 0, calldata, 0, 0, 0, 0,
            ethers.constants.AddressZero,
            ethers.constants.AddressZero,
            signature,
            { gasLimit: 350000, gasPrice: adjustedGasPrice }
        );

        console.log(`   TX submitted: ${tx.hash}`);
        const receipt = await tx.wait();

        if (receipt.status === 1) {
            console.log(`   ✅ Redeemed via Gnosis Safe! Gas: ${receipt.gasUsed.toString()}`);
            return { success: true, txHash: tx.hash };
        } else {
            return { success: false, error: 'Transaction reverted' };
        }
    } catch (error: any) {
        const errMsg = error.message || String(error);
        console.log(`   ❌ ${useNegRisk ? 'NEG_RISK' : 'CTF'} failed: ${errMsg.slice(0, 200)}`);
        if (!useNegRisk && position.negativeRisk !== false) {
            console.log(`   Retrying with NEG_RISK contract via Gnosis Safe...`);
            await sleep(RPC_DELAY);
            return redeemViaGnosisSafe(wallet, proxyAddress, position, true);
        }
        return { success: false, error: errMsg };
    }
};

// Redeem directly from EOA wallet
const redeemDirectly = async (
    wallet: ethers.Wallet,
    position: Position,
    useNegRisk: boolean = false
): Promise<{ success: boolean; txHash?: string; error?: string }> => {
    const ctfAddress = useNegRisk ? NEG_RISK_CTF_ADDRESS : CTF_CONTRACT_ADDRESS;
    const ctfContract = new ethers.Contract(ctfAddress, CTF_ABI, wallet);

    try {
        let conditionId = position.conditionId;
        if (!conditionId.startsWith('0x')) {
            conditionId = '0x' + conditionId;
        }
        const conditionIdBytes32 = ethers.utils.hexZeroPad(conditionId, 32);

        console.log(`   Attempting redemption on ${useNegRisk ? 'NEG_RISK' : 'CTF'} contract...`);

        await sleep(RPC_DELAY);
        const feeData = await wallet.provider!.getFeeData();
        const gasPrice = feeData.gasPrice || feeData.maxFeePerGas;
        const adjustedGasPrice = gasPrice ? gasPrice.mul(120).div(100) : undefined;

        await sleep(RPC_DELAY);
        const tx = await ctfContract.redeemPositions(
            USDC_ADDRESS,
            ethers.constants.HashZero,
            conditionIdBytes32,
            [1, 2],
            { gasLimit: 250000, gasPrice: adjustedGasPrice }
        );

        console.log(`   TX submitted: ${tx.hash}`);
        const receipt = await tx.wait();

        if (receipt.status === 1) {
            console.log(`   ✅ Redeemed! Gas: ${receipt.gasUsed.toString()}`);
            return { success: true, txHash: tx.hash };
        } else {
            return { success: false, error: 'Transaction reverted' };
        }
    } catch (error: any) {
        const errMsg = error.message || String(error);
        console.log(`   ❌ ${useNegRisk ? 'NEG_RISK' : 'CTF'} failed: ${errMsg.slice(0, 200)}`);
        if (!useNegRisk && position.negativeRisk !== false) {
            console.log(`   Retrying with NEG_RISK contract...`);
            await sleep(RPC_DELAY);
            return redeemDirectly(wallet, position, true);
        }
        return { success: false, error: errMsg };
    }
};

const main = async () => {
    console.log('🚀 Redeeming resolved positions');
    console.log('════════════════════════════════════════════════════');
    console.log(`Wallet: ${PROXY_WALLET}`);
    console.log(`CTF Contract: ${CTF_CONTRACT_ADDRESS}`);
    console.log(`Win threshold: price >= $${RESOLVED_HIGH}`);
    console.log(`Loss threshold: price <= $${RESOLVED_LOW}`);

    const provider = new ethers.providers.JsonRpcProvider(RPC_URL);
    const wallet = new ethers.Wallet(PRIVATE_KEY, provider);

    const maskedRpc = RPC_URL.length > 20 ? RPC_URL.slice(0, -20) + '...' : RPC_URL;
    console.log(`\n✅ Connected to Polygon RPC: ${maskedRpc}`);
    console.log(`Signer address: ${wallet.address}`);

    // Detect if proxy wallet is a Gnosis Safe (contract) or EOA
    await sleep(RPC_DELAY);
    const isProxy = await isGnosisSafe(PROXY_WALLET, provider);
    const signerIsProxy = wallet.address.toLowerCase() === PROXY_WALLET.toLowerCase();

    if (isProxy) {
        console.log(`🔐 Proxy wallet is Gnosis Safe - will use execTransaction`);
    } else if (!signerIsProxy) {
        console.log(`⚠️  Signer differs from proxy but proxy is not a contract`);
    } else {
        console.log(`📝 EOA mode - direct redemption`);
    }

    await sleep(RPC_DELAY);
    const maticBalance = await provider.getBalance(wallet.address);
    const maticEth = parseFloat(ethers.utils.formatEther(maticBalance));
    console.log(`MATIC balance: ${maticEth.toFixed(4)}`);

    if (maticEth < 0.01) {
        console.log('❌ Low MATIC! Need gas for transactions.');
        return;
    }

    const allPositions = await loadPositions(PROXY_WALLET);

    if (allPositions.length === 0) {
        console.log('\n🎉 No open positions detected for proxy wallet.');
        return;
    }

    // Redeemable: high price OR API redeemable flag OR has value despite low price
    // After resolution, curPrice drops to ~0 for BOTH winners and losers,
    // so we also check redeemable flag and currentValue to catch resolved winners.
    // On-chain redeem reverts harmlessly if not actually redeemable.
    const redeemablePositions = allPositions.filter(
        (pos) => pos.size > ZERO_THRESHOLD && (
            pos.curPrice >= RESOLVED_HIGH ||
            pos.redeemable === true ||
            (pos.currentValue > ZERO_THRESHOLD && pos.curPrice <= RESOLVED_LOW)
        )
    );
    const activePositions = allPositions.filter(
        (pos) => pos.curPrice > RESOLVED_LOW && pos.curPrice < RESOLVED_HIGH
            && pos.size > ZERO_THRESHOLD && !pos.redeemable
    );
    // Losing = low price, no value, not redeemable
    const losingPositions = allPositions.filter(
        (pos) => pos.curPrice <= RESOLVED_LOW && pos.size > ZERO_THRESHOLD
            && !pos.redeemable && pos.currentValue <= ZERO_THRESHOLD
    );

    console.log(`\n📊 Position statistics:`);
    console.log(`   Total positions: ${allPositions.length}`);
    console.log(`   💰 Redeemable (won / claimable): ${redeemablePositions.length}`);
    console.log(`   🎯 Active (in progress): ${activePositions.length}`);
    console.log(`   💀 Losing (worthless): ${losingPositions.length}`);

    if (redeemablePositions.length === 0) {
        console.log('\n✅ No positions to redeem.');
        return;
    }

    console.log(`\n🔄 Redeeming ${redeemablePositions.length} positions...`);
    console.log(`⚠️  WARNING: Each redemption requires gas fees on Polygon`);

    let successCount = 0;
    let failCount = 0;
    let totalValue = 0;

    const positionsByCondition = new Map<string, Position[]>();
    redeemablePositions.forEach((pos) => {
        const existing = positionsByCondition.get(pos.conditionId) || [];
        existing.push(pos);
        positionsByCondition.set(pos.conditionId, existing);
    });

    console.log(`\n📦 Grouped into ${positionsByCondition.size} unique conditions`);

    let conditionIndex = 0;
    let skippedZeroValue = 0;
    for (const [conditionId, positions] of positionsByCondition.entries()) {
        conditionIndex++;
        // Use size as value for redeemable WINNING positions (API returns currentValue=0 after resolution)
        // Only use size fallback if position is on the winning side (curPrice >= RESOLVED_HIGH)
        const totalPositionValue = positions.reduce((sum, pos) => {
            if (pos.redeemable && pos.currentValue < ZERO_THRESHOLD && pos.curPrice >= RESOLVED_HIGH) {
                return sum + pos.size;  // Winning side, resolved — use token count as value
            }
            return sum + pos.currentValue;
        }, 0);

        // Skip $0 positions — losing side, not worth gas cost
        if (totalPositionValue < ZERO_THRESHOLD) {
            skippedZeroValue++;
            console.log(`   [SKIP] Condition ${conditionIndex}/${positionsByCondition.size}: $0 value — losing side, skip redeem`);
            continue;
        }

        console.log(`\n${'='.repeat(60)}`);
        console.log(`Condition ${conditionIndex}/${positionsByCondition.size}`);
        console.log(`Condition ID: ${conditionId}`);
        console.log(`Positions in this condition: ${positions.length}`);
        console.log(`Total expected value: $${totalPositionValue.toFixed(2)}`);

        positions.forEach((pos) => {
            const status = pos.curPrice >= RESOLVED_HIGH ? '🎉' : '❌';
            console.log(
                `   ${status} ${pos.title || pos.slug} | ${pos.outcome} | ${pos.size.toFixed(2)} tokens | $${pos.currentValue.toFixed(2)}`
            );
        });

        // Route through Gnosis Safe if proxy wallet is a contract, else direct
        const result = isProxy
            ? await redeemViaGnosisSafe(wallet, PROXY_WALLET, positions[0])
            : await redeemDirectly(wallet, positions[0]);

        if (result.success) {
            successCount++;
            totalValue += totalPositionValue;
            console.log(`   ✅ Condition ${conditionIndex} redeemed: $${totalPositionValue.toFixed(2)} | TX: ${result.txHash}`);
        } else {
            failCount++;
            console.log(`   ❌ Condition ${conditionIndex} FAILED: ${(result.error || 'unknown').slice(0, 200)}`);
        }

        if (conditionIndex < positionsByCondition.size) {
            console.log(`   ⏳ Waiting 15s before next transaction...`);
            await sleep(15000);
        }
    }

    console.log('\n════════════════════════════════════════════════════');
    console.log('✅ Summary of position redemption');
    console.log(`Conditions processed: ${positionsByCondition.size} (${skippedZeroValue} skipped $0)`);
    console.log(`Successful redemptions: ${successCount}`);
    console.log(`Failed: ${failCount}`);
    console.log(`Expected value of redeemed positions: $${totalValue.toFixed(2)}`);
    console.log('════════════════════════════════════════════════════\n');
};

// Parse CLI args for loop mode
const args = process.argv.slice(2);
const loopMode = args.includes('--loop') || args.includes('-l');
const intervalSec = parseInt(args.find(a => a.startsWith('--interval='))?.split('=')[1] || '60', 10);

if (loopMode) {
    console.log(`\n🔁 Loop mode enabled - scanning every ${intervalSec}s (Ctrl+C to stop)\n`);
    const runLoop = async () => {
        while (true) {
            try {
                await main();
            } catch (error) {
                console.error('❌ Error in loop:', error);
            }
            console.log(`\n⏳ Next scan in ${intervalSec}s...\n`);
            await new Promise(r => setTimeout(r, intervalSec * 1000));
        }
    };
    runLoop();
} else {
    main()
        .then(() => process.exit(0))
        .catch((error) => {
            console.error('❌ Script aborted due to error:', error);
            process.exit(1);
        });
}
