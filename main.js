const colors = require('colors');
const { exec } = require('child_process');
require('dotenv').config({ quiet: true });

colors.enable();


function log(msg, type = 'info') {
  const timestamp = new Date().toLocaleTimeString();
  let logMessage = `${timestamp} ${msg}`;

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

function runScript(scriptName) {
  return new Promise((resolve, reject) => {
    log(`Đang chạy ${scriptName}...`, 'info');
    const process = exec(`node ${scriptName}`);

    process.stdout.on('data', (data) => {
      console.log(data.toString());
    });

    process.stderr.on('data', (data) => {
      log(data.toString(), 'error');
    });

    process.on('close', (code) => {
      if (code === 0) {
        log(`${scriptName} hoàn tất thành công`, 'success');
        resolve();
      } else {
        log(`${scriptName} thất bại với mã lỗi ${code}`, 'error');
        reject(new Error(`Mã lỗi ${code}`));
      }
    });
  });
}

(async () => {
  log('Khởi động chương trình...', 'info');
  try {
    await runScript('trade.js');
  } catch (error) {
    log(`Lỗi khi chạy trade.js: ${error.message}`, 'error');
  }
  log('Chương trình đã dừng', 'success');
  process.exit(0);
})();

process.on('uncaughtException', (error, origin) => {
  log(`Lỗi không bắt được: ${error.message} (Origin: ${origin})`, 'error');
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  log(`Lỗi Promise không được xử lý: ${reason}`, 'error');
  process.exit(1);
});