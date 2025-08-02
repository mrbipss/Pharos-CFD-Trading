require('dotenv').config({ quiet: true });
const { ethers } = require('ethers');
const colors = require('colors');
const fs = require('fs');
const { EthersWallet } = require('ethersjs3-wallet');
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const axios = require('axios');
const config = require('./config');
const { MAX_THREADS, THREAD_TIMEOUT, SOLAN_TRADE } = config;

const networkConfig = {
  name: 'Pharos Testnet',
  chainId: 688688,
  rpcUrl: 'https://testnet.dplabs-internal.com',
};

const TOKEN_CONTRACT_ADDRESS = '0x78ac5e2d8a78a8b8e6d10c7b7274b03c10c91cef';
const CLAIM_CONTRACT_ADDRESS = '0x50576285bd33261dee1ad99bf766cd8249520a58';
const TRADE_ROUTER_ADDRESS = '0xDe897635870b3Dd2e097C09f1cd08841DBc3976a';
const APPROVE_SPENDER_ADDRESS = '0x9a88d07850723267db386c681646217af7e220d7';
const BASE_API = 'https://proofcrypto-production.up.railway.app';
const wallet = new EthersWallet();

const ERC20_ABI = [
  {
    type: 'function',
    name: 'balanceOf',
    stateMutability: 'view',
    inputs: [{ name: 'owner', type: 'address' }],
    outputs: [{ name: '', type: 'uint256' }],
  },
  {
    type: 'function',
    name: 'decimals',
    stateMutability: 'view',
    inputs: [],
    outputs: [{ name: '', type: 'uint8' }],
  },
  {
    type: 'function',
    name: 'approve',
    stateMutability: 'nonpayable',
    inputs: [
      { name: 'spender', type: 'address' },
      { name: 'amount', type: 'uint256' },
    ],
    outputs: [{ name: '', type: 'bool' }],
  },
  {
    type: 'function',
    name: 'allowance',
    stateMutability: 'view',
    inputs: [
      { name: 'owner', type: 'address' },
      { name: 'spender', type: 'address' },
    ],
    outputs: [{ name: '', type: 'uint256' }],
  },
];

const CLAIM_ABI = [
  {
    type: 'function',
    name: 'claim',
    stateMutability: 'nonpayable',
    inputs: [],
    outputs: [],
    methodID: '0x4e71d92d',
  },
];

const BROKEX_CONTRACT_ABI = [
  {
    name: 'openPosition',
    type: 'function',
    stateMutability: 'nonpayable',
    inputs: [
      { internalType: 'uint256', name: 'idx', type: 'uint256' },
      { internalType: 'bytes', name: 'proof', type: 'bytes' },
      { internalType: 'bool', name: 'isLong', type: 'bool' },
      { internalType: 'uint256', name: 'lev', type: 'uint256' },
      { internalType: 'uint256', name: 'size', type: 'uint256' },
      { internalType: 'uint256', name: 'sl', type: 'uint256' },
      { internalType: 'uint256', name: 'tp', type: 'uint256' },
    ],
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
  },
];

const pairs = [
  { name: 'AAPL_USDT', dc: 6004 },
];

const solantrade = SOLAN_TRADE;
const MAX_TRADE_RETRIES = 100;

class TradeService {
  constructor({ accountIndex, privateKey }) {
    this.provider = new ethers.JsonRpcProvider(networkConfig.rpcUrl);
    this.wallet = privateKey ? new ethers.Wallet(privateKey, this.provider) : null;
    this.accountIndex = accountIndex;
    this.usedNonce = {};
    this.tradeCount = 1; // Mỗi worker chỉ trade 1 lần
    this.approvedSpenders = new Set();
    this.axiosInstance = axios.create();
  }

  async log(msg, type = 'info') {
    const timestamp = new Date().toLocaleTimeString();
    const accountPrefix = `[Tài khoản ${this.accountIndex + 1}]`;
    let logMessage = `${timestamp} ${accountPrefix} ${msg}`;

    switch (type) {
      case 'success':
        console.log(logMessage.green);
        break;
      case 'error':
        console.log(logMessage.red);
        break;
      case 'warning':
        console.log(logMessage.yellow);
        break;
      default:
        console.log(logMessage.blue);
    }
  }

  async getTokenBalance(tokenAddress) {
    try {
      const tokenContract = new ethers.Contract(tokenAddress, ERC20_ABI, this.provider);
      const balance = await tokenContract.balanceOf(this.wallet.address);
      const decimals = await tokenContract.decimals();
      return ethers.formatUnits(balance, decimals);
    } catch (error) {
      await this.log(`Lỗi khi lấy số dư token ${tokenAddress}: ${error.message}`, 'error');
      return null;
    }
  }

  async approveTokenForTrading(spenderAddress = APPROVE_SPENDER_ADDRESS) {
    try {
      const tokenContract = new ethers.Contract(TOKEN_CONTRACT_ADDRESS, ERC20_ABI, this.wallet);
      
      const allowance = await tokenContract.allowance(this.wallet.address, spenderAddress);
      const maxUint256 = ethers.MaxUint256;
      
      if (allowance >= maxUint256 / 2n) {
        await this.log(`Token đã được approve cho ${spenderAddress}`, 'info');
        this.approvedSpenders.add(spenderAddress);
        return true;
      }

      await this.log(`Đang approve token cho spender: ${spenderAddress}`, 'info');
      await this.log(`Allowance hiện tại: ${ethers.formatUnits(allowance, 6)} USDT`, 'info');
      
      const maxFeePerGas = ethers.parseUnits('5', 'gwei');
      const maxPriorityFeePerGas = ethers.parseUnits('2', 'gwei');
      
      let estimatedGas;
      try {
        estimatedGas = await tokenContract.approve.estimateGas(
          spenderAddress, 
          maxUint256,
          { from: this.wallet.address }
        );
      } catch (error) {
        await this.log(`Lỗi khi ước tính gas cho approve: ${error.message}`, 'error');
        return false;
      }

      const approveTx = await tokenContract.approve(spenderAddress, maxUint256, {
        gasLimit: ethers.toBigInt(Math.floor(Number(estimatedGas) * 1.5)),
        maxFeePerGas: maxFeePerGas * 2n,
        maxPriorityFeePerGas: maxPriorityFeePerGas * 2n,
        nonce: ethers.toBigInt(this.usedNonce[this.wallet.address] || await this.provider.getTransactionCount(this.wallet.address, 'pending')),
      });
      
      await this.log(`Approve transaction sent: ${approveTx.hash}`, 'info');
      
      try {
        const receipt = await approveTx.wait(1, 60000);
        this.usedNonce[this.wallet.address] = (this.usedNonce[this.wallet.address] || await this.provider.getTransactionCount(this.wallet.address, 'pending')) + 1;
        
        if (receipt.status === 1) {
          await this.log(`✅ Approve thành công: ${approveTx.hash}`, 'success');
          this.approvedSpenders.add(spenderAddress);
          
          const newAllowance = await tokenContract.allowance(this.wallet.address, spenderAddress);
          await this.log(`✅ Allowance sau approve: ${ethers.formatUnits(newAllowance, 6)} USDT`, 'success');
          return true;
        } else {
          await this.log(`❌ Approve thất bại - transaction reverted`, 'error');
          return false;
        }
      } catch (waitError) {
        await this.log(`Approve timeout nhưng có thể đã thành công: ${waitError.message}`, 'warning');
        this.usedNonce[this.wallet.address] = await this.provider.getTransactionCount(this.wallet.address, 'pending');
        
        const newAllowance = await tokenContract.allowance(this.wallet.address, spenderAddress);
        if (newAllowance >= maxUint256 / 2n) {
          await this.log(`✅ Approve đã thành công (verified by allowance check)`, 'success');
          this.approvedSpenders.add(spenderAddress);
          return true;
        }
        return false;
      }
    } catch (error) {
      await this.log(`❌ Lỗi khi approve token: ${error.message}`, 'error');
      return false;
    }
  }

  async getProof(pair) {
    try {
      const response = await this.axiosInstance.get(`${BASE_API}/proof?pairs=${pair}`, {
        headers: {
          Accept: '*/*',
          'Accept-Language': 'id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7',
          Origin: 'https://app.brokex.trade',
          Referer: 'https://app.brokex.trade/',
          'Sec-Fetch-Dest': 'empty',
          'Sec-Fetch-Mode': 'cors',
          'Sec-Fetch-Site': 'cross-site',
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
        },
      });
      if (response.status === 200) {
        return response.data;
      }
      throw new Error('Không nhận được dữ liệu từ API');
    } catch (error) {
      await this.log(`Lỗi khi lấy proof: ${error.message}`, 'error');
      return null;
    }
  }

  async performTrade(pairIndex, isLong, tradeAmount) {
    for (let attempt = 1; attempt <= MAX_TRADE_RETRIES; attempt++) {
      try {
        await this.log(`Thử trade lần ${attempt}/${MAX_TRADE_RETRIES}`, 'info');
        
        if (!this.approvedSpenders.has(APPROVE_SPENDER_ADDRESS)) {
          await this.log(`🔑 Cần approve token trước khi trade...`, 'info');
          const approveSuccess = await this.approveTokenForTrading(APPROVE_SPENDER_ADDRESS);
          if (!approveSuccess) {
            await this.log(`❌ Approve thất bại, skip trade attempt ${attempt}`, 'error');
            continue;
          }
        }

        const tradeContract = new ethers.Contract(TRADE_ROUTER_ADDRESS, BROKEX_CONTRACT_ABI, this.wallet);
        const tokenContract = new ethers.Contract(TOKEN_CONTRACT_ADDRESS, ERC20_ABI, this.wallet);
        const decimals = 6;
        const tradeAmountWei = ethers.parseUnits(tradeAmount.toString(), decimals);
        
        // Thử lấy proof mới cho mỗi lần retry
        await this.log(`🔄 Đang lấy proof mới cho lần thử ${attempt}...`, 'info');
        const proof = await this.getProof(pairIndex);
        if (!proof || !proof.proof) {
          await this.log('❌ Không thể lấy proof từ API', 'error');
          await new Promise(r => setTimeout(r, 3000)); // Đợi 3s trước khi thử lại
          continue;
        }

        const allowance = await tokenContract.allowance(this.wallet.address, APPROVE_SPENDER_ADDRESS);
        if (allowance < tradeAmountWei) {
          await this.log(`⚠️ Allowance không đủ (${ethers.formatUnits(allowance, 6)} < ${ethers.formatUnits(tradeAmountWei, 6)}), thử approve lại...`, 'warning');
          const reApprove = await this.approveTokenForTrading(APPROVE_SPENDER_ADDRESS);
          if (!reApprove) {
            continue;
          }
        }

        const tradeData = tradeContract.interface.encodeFunctionData('openPosition', [
          pairIndex,
          proof.proof,
          isLong,
          1,
          tradeAmountWei,
          0,
          0,
        ]);

        const maxFeePerGas = ethers.parseUnits('5', 'gwei');
        const maxPriorityFeePerGas = ethers.parseUnits('2', 'gwei');
        
        let estimatedGas;
        try {
          estimatedGas = await tradeContract.openPosition.estimateGas(
            pairIndex, proof.proof, isLong, 1, tradeAmountWei, 0, 0, 
            { from: this.wallet.address }
          );
        } catch (error) {
          await this.log(`❌ Lỗi khi ước tính gas cho trade: ${error.message}`, 'error');
          if (error.message.includes('execution reverted') || error.message.includes('unknown custom error')) {
            await this.log(`⚠️ Proof có thể đã hết hạn hoặc market conditions đã thay đổi, thử lại với proof mới...`, 'warning');
            await new Promise(r => setTimeout(r, 5000)); // Đợi 5s trước khi thử lại
            continue;
          }
          continue;
        }

        const tx = {
          to: TRADE_ROUTER_ADDRESS,
          data: tradeData,
          gasLimit: ethers.toBigInt(Math.floor(Number(estimatedGas) * 1.3)),
          maxFeePerGas: maxFeePerGas * 2n, 
          maxPriorityFeePerGas: maxPriorityFeePerGas * 2n,
          nonce: ethers.toBigInt(this.usedNonce[this.wallet.address] || await this.provider.getTransactionCount(this.wallet.address, 'pending')),
          chainId: ethers.toBigInt(networkConfig.chainId),
        };

        const txHash = await this.sendRawTransactionWithRetries(tx);
        await this.log(`📤 Giao dịch đã được gửi: ${txHash}`, 'info');
        
        try {
          const receipt = await this.waitForReceiptWithRetries(txHash);
          this.usedNonce[this.wallet.address] = (this.usedNonce[this.wallet.address] || await this.provider.getTransactionCount(this.wallet.address, 'pending')) + 1;
          
          await this.log(`🎉 Trade thành công`, 'success');
          await this.log(`📋 Tx Hash: ${txHash}`, 'success');
          return [txHash, receipt.blockNumber];
          
        } catch (receiptError) {
          await this.log(`⚠️ Không thể lấy receipt nhưng giao dịch có thể đã thành công: ${receiptError.message}`, 'warning');
          
          this.usedNonce[this.wallet.address] = await this.provider.getTransactionCount(this.wallet.address, 'pending');
          
          const newBalance = await this.getTokenBalance(TOKEN_CONTRACT_ADDRESS);
          if (newBalance !== null) {
            await this.log(`✅ Kiểm tra balance để xác nhận giao dịch`, 'info');
            return [txHash, null];
          }
        }
        
      } catch (error) {
        await this.log(`❌ [Thử ${attempt}/${MAX_TRADE_RETRIES}] Trade thất bại: ${error.message}`, 'error');
        
        if (attempt < MAX_TRADE_RETRIES) {
          const waitTime = Math.min(5000 + (attempt * 2000), 15000);
          await this.log(`⏳ Chờ ${waitTime/1000} giây trước khi thử lại...`, 'warning');
          await new Promise((resolve) => setTimeout(resolve, waitTime));
        }
        continue;
      }
    }
    
    await this.log(`💥 Không thể thực hiện trade sau ${MAX_TRADE_RETRIES} lần thử`, 'error');
    return [null, null];
  }

  async approveToken(spenderAddress, amount) {
    try {
      const tokenContract = new ethers.Contract(TOKEN_CONTRACT_ADDRESS, ERC20_ABI, this.wallet);
      const decimals = await tokenContract.decimals();
      const amountWei = ethers.parseUnits(amount.toString(), decimals);
      const allowance = await tokenContract.allowance(this.wallet.address, spenderAddress);

      if (allowance < amountWei) {
        await this.log(`Đang phê duyệt ${amount} USDT cho ${spenderAddress}`, 'info');
        const maxFeePerGas = ethers.parseUnits('5', 'gwei');
        const maxPriorityFeePerGas = ethers.parseUnits('2', 'gwei');
        let estimatedGas;
        try {
          estimatedGas = await tokenContract.approve.estimateGas(spenderAddress, ethers.MaxUint256, { from: this.wallet.address });
        } catch (error) {
          await this.log(`Lỗi khi ước tính gas cho phê duyệt: ${error.message}`, 'error');
          return false;
        }

        const tx = await tokenContract.approve(spenderAddress, ethers.MaxUint256, {
          gasLimit: ethers.toBigInt(Math.floor(Number(estimatedGas) * 1.2)),
          maxFeePerGas,
          maxPriorityFeePerGas,
          nonce: ethers.toBigInt(this.usedNonce[this.wallet.address] || await this.provider.getTransactionCount(this.wallet.address, 'pending')),
        });
        await tx.wait();
        this.usedNonce[this.wallet.address] = (this.usedNonce[this.wallet.address] || await this.provider.getTransactionCount(this.wallet.address, 'pending')) + 1;
        await this.log(`Phê duyệt thành công: ${tx.hash}`, 'success');
      } else {
        await this.log(`Token đã được phê duyệt cho ${spenderAddress}`, 'info');
      }
      return true;
    } catch (error) {
      await this.log(`Lỗi khi phê duyệt token: ${error.message}`, 'error');
      return false;
    }
  }

  async sendRawTransactionWithRetries(tx, retries = 5) {
    let maxFeePerGas = ethers.parseUnits('5', 'gwei');
    let maxPriorityFeePerGas = ethers.parseUnits('2', 'gwei');
    let gasBumpCount = 0;
    for (let attempt = 0; attempt < retries; attempt++) {
      try {
        tx.maxFeePerGas = maxFeePerGas;
        tx.maxPriorityFeePerGas = maxPriorityFeePerGas;
        const txResponse = await this.wallet.sendTransaction(tx);
        return txResponse.hash;
      } catch (error) {
        // Nếu gặp TX_REPLAY_ATTACK thì nghỉ 30s rồi thử lại
        if (
          (typeof error.message === 'string' && error.message.includes('TX_REPLAY_ATTACK')) ||
          (error.code === -32600 && error.message && error.message.includes('TX_REPLAY_ATTACK'))
        ) {
          await this.log(`[Thử ${attempt + 1}] Gặp TX_REPLAY_ATTACK, nghỉ 30 giây rồi thử lại...`, 'warning');
          await new Promise(r => setTimeout(r, 30000));
          continue;
        }
        if (error.code === 'NONCE_EXPIRED' || error.code === 'REPLACEMENT_UNDERPRICED') {
          this.usedNonce[this.wallet.address] = await this.provider.getTransactionCount(this.wallet.address, 'pending');
          tx.nonce = ethers.toBigInt(this.usedNonce[this.wallet.address]);
          await this.log(`[Thử ${attempt + 1}] Lỗi gửi giao dịch, cập nhật nonce: ${error.message}`, 'warning');
          continue;
        }
        if (error.code === 'INSUFFICIENT_GAS' || error.code === 'GAS_TOO_LOW') {
          // Đã bỏ dynamic gas, chỉ tăng gas cố định
        }
        // Nếu lỗi, tăng gas lên 20% tối đa 3 lần
        if (gasBumpCount < 3) {
          maxFeePerGas = maxFeePerGas * 12n / 10n;
          maxPriorityFeePerGas = maxPriorityFeePerGas * 12n / 10n;
          gasBumpCount++;
          await this.log(`[Thử ${attempt + 1}] Tăng gas lên 20% (lần ${gasBumpCount}): maxFeePerGas=${ethers.formatUnits(maxFeePerGas, 'gwei')}, maxPriorityFeePerGas=${ethers.formatUnits(maxPriorityFeePerGas, 'gwei')}`, 'warning');
        }
        await this.log(`[Thử ${attempt + 1}] Lỗi gửi giao dịch: ${error.message}`, 'warning');
        await new Promise((resolve) => setTimeout(resolve, 2 ** attempt * 1000));
      }
    }
    throw new Error('Không thể gửi giao dịch sau số lần thử tối đa');
  }

  async waitForReceiptWithRetries(txHash, retries = 10) {
    for (let attempt = 0; attempt < retries; attempt++) {
      try {
        const receipt = await this.provider.waitForTransaction(txHash, 1, 300000); // 5 phút
        if (receipt) return receipt;
      } catch (error) {
        await this.log(`[Thử ${attempt + 1}] Lỗi chờ biên nhận: ${error.message}`, 'warning');
        
        if (error.code === 'TIMEOUT') {
          try {
            const txStatus = await this.checkTransactionStatus(txHash);
            if (txStatus) {
              await this.log('Giao dịch đã được xác nhận qua phương thức kiểm tra thay thế', 'success');
              return txStatus;
            }
          } catch (checkError) {
            await this.log(`Không thể kiểm tra trạng thái giao dịch: ${checkError.message}`, 'warning');
          }
        }
        
        if (error.code === 'TRANSACTION_NOT_FOUND') {
          await this.log('Giao dịch không tìm thấy, có thể đã bị drop khỏi mempool', 'warning');
          continue;
        }
        
        const waitTime = Math.min(2 ** attempt * 1000, 10000);
        await this.log(`Chờ ${waitTime/1000} giây trước khi thử lại...`, 'info');
        await new Promise((resolve) => setTimeout(resolve, waitTime));
      }
    }
    // Kiểm tra lại trạng thái giao dịch lần cuối
    const receipt = await this.provider.getTransactionReceipt(txHash);
    if (receipt) return receipt;
    throw new Error('Không tìm thấy biên nhận giao dịch sau số lần thử tối đa');
  }

  async checkTransactionStatus(txHash) {
    try {
      const tx = await this.provider.getTransaction(txHash);
      if (!tx) {
        await this.log('Giao dịch không tồn tại', 'warning');
        return null;
      }
      
      const receipt = await this.provider.getTransactionReceipt(txHash);
      if (receipt) {
        await this.log(`Tìm thấy biên nhận qua phương thức thay thế`, 'success');
        return receipt;
      }
      
      const currentBlock = await this.provider.getBlockNumber();
      await this.log(`Block hiện tại: ${currentBlock}, Giao dịch vẫn pending`, 'info');
      
      return null;
    } catch (error) {
      await this.log(`Lỗi khi kiểm tra trạng thái giao dịch: ${error.message}`, 'error');
      return null;
    }
  }

  async performClaim() {
    try {
      const claimContract = new ethers.Contract(CLAIM_CONTRACT_ADDRESS, CLAIM_ABI, this.wallet);
      const claimData = claimContract.interface.encodeFunctionData('claim');
      const maxFeePerGas = ethers.parseUnits('5', 'gwei');
      const maxPriorityFeePerGas = ethers.parseUnits('2', 'gwei');
      let estimatedGas;

      try {
        estimatedGas = await claimContract.claim.estimateGas({ from: this.wallet.address });
      } catch (error) {
        await this.log(`Lỗi khi ước tính gas: ${error.message}${error.reason ? `, Reason: ${error.reason}` : ''}`, 'error');
        return [null, null];
      }

      const tx = {
        to: CLAIM_CONTRACT_ADDRESS,
        data: claimData,
        gasLimit: ethers.toBigInt(Math.floor(Number(estimatedGas) * 1.2)),
        maxFeePerGas,
        maxPriorityFeePerGas,
        nonce: ethers.toBigInt(this.usedNonce[this.wallet.address] || await this.provider.getTransactionCount(this.wallet.address, 'pending')),
        chainId: ethers.toBigInt(networkConfig.chainId),
      };

      const txHash = await this.sendRawTransactionWithRetries(tx);
      const receipt = await this.waitForReceiptWithRetries(txHash);
      this.usedNonce[this.wallet.address] = (this.usedNonce[this.wallet.address] || await this.provider.getTransactionCount(this.wallet.address, 'pending')) + 1;

      await this.log(`Claim thành công`, 'success');
      await this.log(`Tx Hash: ${txHash}`, 'success');
      return [txHash, receipt.blockNumber];
    } catch (error) {
      await this.log(`Claim thất bại: ${error.message}${error.reason ? `, Reason: ${error.reason}` : ''}`, 'error');
      return [null, null];
    }
  }

  async processAccount() {
    try {
      await this.log(`Đang xử lý ví: ${this.wallet.address.slice(0, 6)}...${this.wallet.address.slice(-6)}`, 'info');

      await this.log('Sử dụng kết nối trực tiếp', 'info');

      this.usedNonce[this.wallet.address] = await this.provider.getTransactionCount(this.wallet.address, 'pending');

      let balance = await this.getTokenBalance(TOKEN_CONTRACT_ADDRESS);
      if (balance === null) {
        await this.log('Không thể kiểm tra số dư token', 'error');
        return false;
      }

      await this.log(`Số dư token: ${balance} USDT`, 'info');

      if (parseFloat(balance) === 0) {
        await this.log('Số dư token bằng 0, đang gọi hàm claim...', 'info');
        const [txHash, blockNumber] = await this.performClaim();
        if (!txHash || !blockNumber) {
          await this.log('Claim thất bại, không thể tiếp tục trade', 'error');
          return false;
        }
        await this.log('Claim hoàn tất thành công', 'success');
        balance = await this.getTokenBalance(TOKEN_CONTRACT_ADDRESS);
        if (balance === null) {
          await this.log('Không thể kiểm tra số dư token sau claim', 'error');
          return false;
        }
        await this.log(`Số dư token sau claim: ${balance} USDT`, 'info');
      }

      await this.log(`🔑 Đang approve token cho trading...`, 'info');
      const initialApprove = await this.approveTokenForTrading(APPROVE_SPENDER_ADDRESS);
      if (!initialApprove) {
        await this.log('❌ Không thể approve token, dừng quá trình trading', 'error');
        return false;
      }

      let successfulTrades = 0;
      let failedTrades = 0;

      for (let i = 0; i < this.tradeCount; i++) {
        await this.log(`Đang thực hiện trade ${i + 1}/${this.tradeCount}`, 'info');
        const pair = pairs[Math.floor(Math.random() * pairs.length)];
        const isLong = Math.random() > 0.5;
        const action = isLong ? 'Long' : 'Short';
        
        // Kiểm tra balance trước khi trade
        balance = await this.getTokenBalance(TOKEN_CONTRACT_ADDRESS);
        if (balance === null) {
          await this.log('Không thể kiểm tra số dư token', 'error');
          failedTrades++;
          continue; // Tiếp tục với trade tiếp theo
        }

        // Tính toán số tiền trade dựa trên balance
        let tradeAmount;
        if (parseFloat(balance) >= 15) {
          tradeAmount = (Math.random() * (15 - 10) + 10).toFixed(6);
        } else if (parseFloat(balance) >= 5) {
          tradeAmount = (Math.random() * (parseFloat(balance) - 1) + 1).toFixed(6);
        } else if (parseFloat(balance) >= 1) {
          tradeAmount = parseFloat(balance).toFixed(6);
        } else {
          await this.log(`Số dư quá thấp (${balance} USDT), bỏ qua trade này`, 'warning');
          failedTrades++;
          continue; // Tiếp tục với trade tiếp theo
        }

        await this.log(`Số dư hiện tại: ${balance} USDT`, 'info');
        await this.log(`Số lượng trade: ${tradeAmount} USDT`, 'info');
        await this.log(`Cặp: ${action} - ${pair.name}`, 'info');

        const [txHash, blockNumber] = await this.performTrade(pair.dc, isLong, tradeAmount);
        if (txHash && blockNumber) {
          await this.log(`Trade ${i + 1} hoàn tất thành công`, 'success');
          successfulTrades++;
          
          // Cập nhật balance sau trade thành công
          balance = await this.getTokenBalance(TOKEN_CONTRACT_ADDRESS);
          if (balance === null) {
            await this.log('Không thể kiểm tra số dư token sau trade', 'error');
          }
        } else {
          await this.log(`Trade ${i + 1} thất bại sau ${MAX_TRADE_RETRIES} lần thử`, 'error');
          failedTrades++;
        }

        const delay = Math.floor(Math.random() * (10 - 5 + 1)) + 5;
        await this.log(`Chờ ${delay} giây trước khi thực hiện trade tiếp theo...`, 'info');
        await new Promise((resolve) => setTimeout(resolve, delay * 1000));
      }

      await this.log(`📊 Tổng kết: ${successfulTrades} trade thành công, ${failedTrades} trade thất bại`, 'info');
      return successfulTrades > 0; // Trả về true nếu có ít nhất 1 trade thành công
    } catch (error) {
      await this.log(`Xử lý ví thất bại: ${error.message}${error.reason ? `, Reason: ${error.reason}` : ''}`, 'error');
      return false;
    }
  }

  async processSingleTrade(tradeIndex) {
    try {
      await this.log(`Đang xử lý ví: ${this.wallet.address.slice(0, 6)}...${this.wallet.address.slice(-6)}`, 'info');
      await this.log(`Lượt trade ${tradeIndex + 1}`, 'info');
      await this.log('Sử dụng kết nối trực tiếp', 'info');
      this.usedNonce[this.wallet.address] = await this.provider.getTransactionCount(this.wallet.address, 'pending');
      let balance = await this.getTokenBalance(TOKEN_CONTRACT_ADDRESS);
      if (balance === null) {
        await this.log('Không thể kiểm tra số dư token', 'error');
        return false;
      }
      await this.log(`Số dư token: ${balance} USDT`, 'info');
      if (parseFloat(balance) === 0) {
        await this.log('Số dư token bằng 0, đang gọi hàm claim...', 'info');
        const [txHash, blockNumber] = await this.performClaim();
        if (!txHash || !blockNumber) {
          await this.log('Claim thất bại, không thể tiếp tục trade', 'error');
          return false;
        }
        await this.log('Claim hoàn tất thành công', 'success');
        balance = await this.getTokenBalance(TOKEN_CONTRACT_ADDRESS);
        if (balance === null) {
          await this.log('Không thể kiểm tra số dư token sau claim', 'error');
          return false;
        }
        await this.log(`Số dư token sau claim: ${balance} USDT`, 'info');
      }
      await this.log(`🔑 Đang approve token cho trading...`, 'info');
      let approveSuccess = false;
      for (let approveAttempt = 1; approveAttempt <= 5; approveAttempt++) {
        approveSuccess = await this.approveTokenForTrading(APPROVE_SPENDER_ADDRESS);
        if (approveSuccess) break;
        await this.log(`❌ Approve thất bại lần ${approveAttempt}, thử lại...`, 'warning');
        await new Promise(r => setTimeout(r, 3000));
      }
      if (!approveSuccess) {
        await this.log('❌ Không thể approve token sau 5 lần thử, bỏ qua trade này', 'error');
        return;
      }
      // Kiểm tra balance trước khi trade
      balance = await this.getTokenBalance(TOKEN_CONTRACT_ADDRESS);
      if (balance === null) {
        await this.log('Không thể kiểm tra số dư token', 'error');
        return;
      }
      // Tính toán số tiền trade dựa trên balance
      let tradeAmount;
      if (parseFloat(balance) >= 15) {
        tradeAmount = (Math.random() * (15 - 10) + 10).toFixed(6);
      } else if (parseFloat(balance) >= 5) {
        tradeAmount = (Math.random() * (parseFloat(balance) - 1) + 1).toFixed(6);
      } else if (parseFloat(balance) >= 1) {
        tradeAmount = parseFloat(balance).toFixed(6);
      } else {
        await this.log(`Số dư quá thấp (${balance} USDT), bỏ qua trade này`, 'warning');
        return;
      }
      const pair = pairs[Math.floor(Math.random() * pairs.length)];
      const isLong = Math.random() > 0.5;
      const action = isLong ? 'Long' : 'Short';
      await this.log(`Số dư hiện tại: ${balance} USDT`, 'info');
      await this.log(`Số lượng trade: ${tradeAmount} USDT`, 'info');
      await this.log(`Cặp: ${action} - ${pair.name}`, 'info');
      const [txHash, blockNumber] = await this.performTrade(pair.dc, isLong, tradeAmount);
      if (txHash && blockNumber) {
        await this.log(`Trade lượt ${tradeIndex + 1} hoàn tất thành công`, 'success');
      } else {
        await this.log(`Trade lượt ${tradeIndex + 1} thất bại sau ${MAX_TRADE_RETRIES} lần thử`, 'error');
      }
    } catch (error) {
      await this.log(`Xử lý ví thất bại: ${error.message}${error.reason ? `, Reason: ${error.reason}` : ''}`, 'error');
      return false;
    }
  }
}

if (isMainThread) {
  async function runWorkerPool(wallets, tradeIndex) {
    return new Promise((resolve) => {
      let next = 0;
      let running = 0;
      const total = wallets.length;
      function startWorker() {
        if (next >= total) return;
        const accountIndex = next;
        const privateKey = wallets[next];
        next++;
        running++;
        const worker = new Worker(__filename, {
          workerData: {
            accountIndex,
            privateKey,
            tradeIndex,
          },
        });
        const timeout = setTimeout(() => {
          worker.terminate();
          console.log(`[Worker ${accountIndex}] Luồng đã hết thời gian sau 20 phút`.red);
        }, THREAD_TIMEOUT);
        worker.on('message', (msg) => console.log(msg));
        worker.on('error', (err) => console.log(`[Worker ${accountIndex}] Lỗi luồng: ${err.message}`.red));
        worker.on('exit', (code) => {
          clearTimeout(timeout);
          running--;
          startWorker(); // worker xong thì lấy ví tiếp theo
          if (running === 0 && next >= total) {
            resolve();
          }
          console.log(`[Worker ${accountIndex}] Luồng đã thoát với mã ${code}`.blue);
        });
      }
      // Khởi tạo tối đa MAX_THREADS worker đầu tiên
      for (let i = 0; i < MAX_THREADS && i < total; i++) {
        startWorker();
      }
    });
  }

  async function main() {
    const walletData = fs.readFileSync('wallet.txt', 'utf8');
    const privateKeys = walletData
      .split(/\r?\n/)
      .map(key => key.trim())
      .filter(key => key !== '' && key.length > 0);

    for (let tradeIndex = 0; tradeIndex < SOLAN_TRADE; tradeIndex++) {
      await runWorkerPool(privateKeys, tradeIndex);
      console.log(`🎉 Lượt trade ${tradeIndex + 1} cho tất cả ví đã hoàn tất`.blue);
      await new Promise((resolve) => setTimeout(resolve, 3000)); // Nghỉ 3s giữa các lượt
    }
    console.log('🎉 TẤT CẢ HOẠT ĐỘNG ĐÃ HOÀN TẤT!'.blue);
  }
  main().catch((err) => console.error('Lỗi rồi:'.red, err));
} else {
  // Worker chỉ trade 1 lần cho 1 ví
  const { accountIndex, privateKey, tradeIndex } = workerData;
  (async () => {
    const bot = new TradeService({ accountIndex, privateKey });
    await bot.processSingleTrade(tradeIndex);
    parentPort.postMessage('Hoàn thành');
  })().catch((err) => parentPort.postMessage(`Lỗi worker: ${err.message}`));
}

process.on('SIGINT', () => {
  console.log('🛑 Bot đã dừng lại'.red);
  process.exit(0);
});

process.on('uncaughtException', (error) => {
  console.error('💥 Uncaught Exception:'.red, error.message, error.stack);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('💥 Unhandled Rejection at:'.red, promise, 'reason:', reason.message, reason.stack);
  process.exit(1);
});