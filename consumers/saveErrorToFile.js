const fs = require('fs');
const path = require('path');

async function saveErrorToFile(content, topic, err) {
  try {
    const errorsDir = path.join(__dirname, '..', 'errorsSave');
    await fs.promises.mkdir(errorsDir, { recursive: true });

    const fileName = `error_${Date.now()}_${Math.random().toString(36).slice(2,10)}.json`;
    const errorFilePath = path.join(errorsDir, fileName);

    const errorData = {
      timestamp: new Date().toISOString(),
      topic,
      content,
      error: err ? err.message : 'Insert failed (no error message)',
    };

    await fs.promises.writeFile(errorFilePath, JSON.stringify(errorData, null, 2), 'utf8');
    console.log('Error data saved to file:', errorFilePath);
  } catch (fileErr) {
    console.error('Failed to write error file:', fileErr);
  }
}

module.exports = { saveErrorToFile }
