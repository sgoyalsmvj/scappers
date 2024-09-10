import fs from 'fs';
import https from 'https';
import path from 'path';
import { fileURLToPath } from 'url';

// Get __dirname for ESM
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Ensure downloads folder exists
const downloadsFolder = path.join(__dirname, 'downloads');
if (!fs.existsSync(downloadsFolder)) {
  fs.mkdirSync(downloadsFolder);
}

// Function to download a file
const downloadFile = (url, destination) => {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(destination);
    https.get(url, (response) => {
      if (response.statusCode !== 200) {
        reject(`Failed to get '${url}' (${response.statusCode})`);
        return;
      }
      response.pipe(file);
      file.on('finish', () => {
        file.close(resolve);  // Close the file and resolve the promise
      });
    }).on('error', (error) => {
      fs.unlink(destination, () => {}); // Delete the file if there's an error
      reject(error.message);
    });
  });
};

// Read downloadState.json to get the last processed line number
const readState = () => {
  const stateFile = path.join(__dirname, 'downloadState.json');
  if (fs.existsSync(stateFile)) {
    return JSON.parse(fs.readFileSync(stateFile, 'utf8')).lastProcessedLine;
  }
  return -1;  // If no state file, start from the beginning
};

// Save the current line number to downloadState.json
const saveState = (lineNumber) => {
  const stateFile = path.join(__dirname, 'downloadState.json');
  fs.writeFileSync(stateFile, JSON.stringify({ lastProcessedLine: lineNumber }, null, 2));
};

// Read download.txt to get the list of URLs
const getDownloadUrls = () => {
  const downloadFile = path.join(__dirname, 'downloadfinal.txt');
  return fs.readFileSync(downloadFile, 'utf8').split('\n').map(line => line.trim()).filter(line => line);
};

const main = async () => {
  let lastProcessedLine = readState();
  const urls = getDownloadUrls();

  for (let i = lastProcessedLine + 1; i < urls.length; i++) {
    const url = urls[i];
    const fileName = path.basename(url);  // Get the file name from the URL
    const destination = path.join(downloadsFolder, fileName); // Save to downloads folder

    try {
      console.log(`Downloading: ${url}`);
      await downloadFile(url, destination);
      console.log(`Successfully downloaded to ${destination}`);
      
      // Save the state after each successful download
      saveState(i);
    } catch (error) {
      console.error(`Error downloading ${url}: ${error}`);
      break;  // Stop on error, can be changed to continue for next URL
    }
  }
};

main();
