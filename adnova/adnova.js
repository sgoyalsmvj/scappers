import axios from "axios";
import { v4 as uuidv4 } from "uuid";
import { countryList, countryToLanguage, dataList } from "../datalist.js";
import fs from "fs";
import pkg from "pg"; // Updated pg import to handle CommonJS module
import { awsConfig, s3Config, dbConfig } from "./variables.js";
const { Pool } = pkg;

const pool = new Pool(dbConfig);
const readState = () => {
  if (fs.existsSync(stateFile)) {
    const state = JSON.parse(fs.readFileSync(stateFile, "utf8"));
    return state;
  }
  return { lastProcessedIndex: -1 }; // Default to -1 if no state exists
};
// Function to save the last processed index (state)
const saveState = (index) => {
  fs.writeFileSync(stateFile, JSON.stringify({ lastProcessedIndex: index }));
};
// Function to write the batch data to a JSON file
const writeBatchToFile = (batch) => {
  if (!fs.existsSync(outputFile)) {
    // Create a new file with the first batch
    fs.writeFileSync(outputFile, JSON.stringify(batch, null, 2));
  } else {
    // Append new batch to existing file
    const existingData = JSON.parse(fs.readFileSync(outputFile, "utf8"));
    const newData = existingData.concat(batch); // Merge old and new data
    fs.writeFileSync(outputFile, JSON.stringify(newData, null, 2));
  }
};

import path from "path"; // ESM me import
import { fileURLToPath } from "url";
const loggedUrls = new Set();
// __dirname ko replicate karne ke liye
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Now define your file paths
const stateFile = path.join(__dirname, "state.json");
const outputFile = path.join(__dirname, "adsData.json");
const downloadFile = path.join(__dirname, "downloadfinal.txt"); // For downloadfinal.txt file

// Update your replaceUrls function accordingly
const replaceUrls = (url) => {
  // Define patterns and replacement
  const patterns = [
    "https://d1uzq5fbagtslj.cloudfront.net/inspiration_brand_logo/",
    "https://d1uzq5fbagtslj.cloudfront.net/__inspiration/assets/",
  ];
  const replacement = "https://d1br1uj2vallv9.cloudfront.net/graphics/";

  let newUrl = url;

  // Check each pattern to see if it matches the URL
  for (const pattern of patterns) {
    if (url && url.startsWith(pattern)) {
      // Replace the URL
      newUrl = url.replace(pattern, replacement);

      // Log the old URL to downloadfinal.txt and mark it as logged
      if (!loggedUrls.has(url)) {
        fs.appendFileSync(downloadFile, `${url}\n`);
        loggedUrls.add(url);
      }
      break; // Exit loop once a match is found
    }
  }

  return newUrl;
};
let loginToken =
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjAxSjdCQVFBRVJDS0E1RDM5MjE3M1g0UTdKIiwiZXhwaXJlZEF0IjoxNzI2NTQ4NDQyNDY0LCJpYXQiOjE3MjU5NDM2NDIsImV4cCI6MTcyNjU0ODQ0Mn0.TCUgELTuySEYXCK5ijTRxmcSan8Qhouuy7T8pWJ5wIE";

const adnovaLogin = async () => {
  try {
    let data = JSON.stringify({
      email: "famedeby@teleg.eu",
      password: "qwerty@123",
    });

    let config = {
      method: "post",
      maxBodyLength: Infinity,
      url: "https://app.adnova.ai/creativeops-auth/api/v1/login",
      headers: {
        accept: "application/json",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "no-cache",
        "content-type": "application/json",
        cookie:
          "intercom-device-id-d2ns7pbx=09aa8075-9b12-4a2b-9fed-b547ee9dc470; intercom-id-d2ns7pbx=ade2c01b-33ac-4e4c-aba8-507c85c2fc4a; intercom-session-d2ns7pbx=; _hp2_id.3887860288=%7B%22userId%22%3A%222841703592765240%22%2C%22pageviewId%22%3A%224537849009752876%22%2C%22sessionId%22%3A%228349115378447943%22%2C%22identity%22%3Anull%2C%22trackerVersion%22%3A%224.0%22%7D; _hp2_ses_props.3887860288=%7B%22r%22%3A%22https%3A%2F%2Fadnova.ai%2F%22%2C%22ts%22%3A1725883500056%2C%22d%22%3A%22app.adnova.ai%22%2C%22h%22%3A%22%2Flogin%22%7D",
        origin: "https://app.adnova.ai",
        pragma: "no-cache",
        priority: "u=1, i",
        referer: "https://app.adnova.ai/login",
        "sec-ch-ua":
          '"Chromium";v="128", "Not;A=Brand";v="24", "Brave";v="128"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "sec-gpc": "1",
        "user-agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
      },
      data: data,
    };
    const resp = await axios.request(config);
    if (resp.data.token) {
      loginToken = resp.data.token;
    }
  } catch (error) {
    console.log(error);
  }
};

const adnovaAdsFetch = async (pagetoken, major_theme) => {
  try {
    let data = JSON.stringify({
      sortBy: "recomended",
      searchQuery: {
        field: "ad_details",
        term: major_theme,
      },
    });
    if (pagetoken) {
      data = JSON.stringify({
        searchQuery: {
          field: "ad_details",
          term: major_theme,
        },
        others: {
          pageToken: pagetoken,
        },
        sortBy: "recomended",
      });
    }

    let config = {
      method: "post",
      maxBodyLength: Infinity,
      url: "https://app.adnova.ai/creativeops/api/v1/inspiration/library/search?page=1",
      headers: {
        accept: "application/json",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "no-cache",
        "content-type": "application/json",
        cookie:
          "intercom-device-id-d2ns7pbx=09aa8075-9b12-4a2b-9fed-b547ee9dc470; intercom-id-d2ns7pbx=ade2c01b-33ac-4e4c-aba8-507c85c2fc4a; _hp2_id.3887860288=%7B%22userId%22%3A%222841703592765240%22%2C%22pageviewId%22%3A%222748724373847258%22%2C%22sessionId%22%3A%227798029140921190%22%2C%22identity%22%3Anull%2C%22trackerVersion%22%3A%224.0%22%7D; _hp2_ses_props.3887860288=%7B%22r%22%3A%22https%3A%2F%2Fadnova.ai%2F%22%2C%22ts%22%3A1725943370523%2C%22d%22%3A%22app.adnova.ai%22%2C%22h%22%3A%22%2Fapp%2Flibrary%2Fdiscover%22%7D; intercom-session-d2ns7pbx=dEJTTnVjR0RmeU91U2VyME8ybEF0SitTdDV4N3NuRG9nMWtjYzBuaXRVMlR1dmU5T0o5NGtJOHVIUVEyZlQ5eC0tRXUwVDVHVW8rQnlnL3ZlRHpod29vdz09--85dd89ada45826af28ef5e454aaeee9e66065757",
        origin: "https://app.adnova.ai",
        pragma: "no-cache",
        priority: "u=1, i",
        referer: "https://app.adnova.ai/app/library/discover",
        "sec-ch-ua":
          '"Chromium";v="128", "Not;A=Brand";v="24", "Brave";v="128"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "sec-gpc": "1",
        "user-agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "x-auth-token": loginToken,
      },
      data: data,
    };

    const resp = await axios.request(config);
    if (resp.data.files.results.length === 0) {
      console.log("No data found for major theme:", major_theme);
      return {
        adsData: [],
        pagetoken: null,
      };
    }
    return {
      adsData: resp.data.files.results,
      pagetoken: resp.data.files.others.nextPageToken,
    };
  } catch (error) {
    console.log(error);
  }
};

function mapNewData(newData, category, major_theme) {
  const validateAndStringifyJson = (data) => {
    try {
      if (data && typeof data === "object") {
        return JSON.stringify(data); // Convert valid objects to JSON strings
      }
      return null;
    } catch (error) {
      console.error("Invalid JSON data:", data);
      return null;
    }
  };

  const formatDateFromTimestamp = (timestamp, includeMilliseconds = false) => {
    const date = new Date(timestamp * 1000); // Convert seconds to milliseconds

    const year = date.getUTCFullYear();
    const month = String(date.getUTCMonth() + 1).padStart(2, "0"); // Months are 0-based
    const day = String(date.getUTCDate()).padStart(2, "0");
    const hours = String(date.getUTCHours()).padStart(2, "0");
    const minutes = String(date.getUTCMinutes()).padStart(2, "0");
    const seconds = String(date.getUTCSeconds()).padStart(2, "0");

    if (includeMilliseconds) {
      const milliseconds = String(date.getUTCMilliseconds()).padStart(6, "0");
      return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}.${milliseconds}`;
    } else {
      return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
    }
  };

  const formatArrayForPostgres = (arr) => {
    if (Array.isArray(arr)) {
      // Escape quotes and format array elements
      return `{${arr
        .map((item) => `"${item.replace(/"/g, '\\"')}"`)
        .join(",")}}`;
    }
    return null;
  };

  console.log("Mapping data");

  const asset = newData.assets?.[0] || {}; // Ensure asset exists and use default empty object if not

  return {
    id: uuidv4(), // Ensure UUID generation function is available
    family: "S_ANV",
    objectID: newData.ad_id?.toString() || null,
    brand: validateAndStringifyJson({
      id: newData.brandId || null,
      name: newData.page_name || null,
      avatar_url: newData.page_profile_picture_url || null,
      website_url: asset.link_url || null,
    }),
    ad_external_link: asset.link_url || null,
    country: formatArrayForPostgres(getRandomCountries(4)) || null,
    ad: validateAndStringifyJson({
      id: newData.ad_id?.toString() || null,
      display_format: newData.display_format || null,
      body: asset.body || null,
      title: asset.title || null,
      status: newData.is_active ? "active" : "inactive",
      start_date: newData.start_date
        ? formatDateFromTimestamp(newData.start_date)
        : null,
      end_date: newData.end_date
        ? formatDateFromTimestamp(newData.end_date, true)
        : null,
      platforms: newData.publisher_platform || [], // Ensure it's an array
      body: asset.body || null,
      caption: null,
      cta_type: asset.cta_type || null,
      cta_text: asset.cta_text || null,
      link_url: asset.link_url || null,
      link_description: asset.link_description || null,
    }),
    ad_status: newData.is_active ? "active" : "inactive",
    ad_start_date: newData.start_date
      ? formatDateFromTimestamp(newData.start_date)
      : null,
    ad_end_date: newData.end_date
      ? formatDateFromTimestamp(newData.end_date, true)
      : null,
    ad_platforms: formatArrayForPostgres(newData.publisher_platform) || null,
    ad_display_format: newData.display_format?.toLowerCase() || null,
    analysis: null, // Ensure proper formatting for JSON
    s3_image_url:
      (newData.display_format === "image" && replaceUrls(asset.mediaKey)) ||
      null,
    s3_video_url:
      (newData.display_format === "video" && replaceUrls(asset.mediaKey)) ||
      null,
    ad_image: null,
    ad_video: null,
    category: category || null,
    language: null,
    theme: null,
    created_at: newData.createdAt
      ? new Date(newData.createdAt).toISOString()
      : null,
    updated_at: new Date().toISOString(),
    likes: newData.likes || null,
    preview_image_url: replaceUrls(asset.thumbnailKey) || null,
    title: asset.title || null,
    video_len: asset.size || null,
    saved_details: null,
    ad_categories: null, // Ensure proper formatting for JSON
    marketing_goals: validateAndStringifyJson(null), // Ensure proper formatting for JSON
    rating: getRandomRating(), // Make sure to implement getRandomRating function
    report: null, // Ensure proper formatting for JSON
    major_theme: major_theme || null,
    sub_theme: null,
    spend: null,
    impression: null,
    focus_area: null,
    ad_body: asset.body || null,
    ad_body_tsv: null, // Remove to_tsvector for now
    brand_name: newData.page_name || null,
  };
}

async function saveBatchToDatabase(batch) {
  const columns = [
    '"id"',
    '"family"',
    '"objectID"',
    '"brand"',
    '"country"',
    '"ad"',
    '"ad_status"',
    '"ad_start_date"',
    '"ad_end_date"',
    '"ad_platforms"',
    '"ad_display_format"',
    '"ad_external_link"',
    '"s3_image_url"',
    '"s3_video_url"',
    '"ad_image"',
    '"ad_video"',
    '"category"',
    '"tiktok_reach"',
    '"language"',
    '"theme"',
    '"saved_details"',
    '"ad_categories"',
    '"likes"',
    '"marketing_goals"',
    '"rating"',
    '"preview_image_url"',
    '"report"',
    '"major_theme"',
    '"sub_theme"',
    '"spend"',
    '"impression"',
    '"focus_area"',
    '"video_len"',
    '"ad_body"',
    '"brand_name"',
    '"title"',
    '"analysis"',
    '"created_at"',
    '"updated_at"',
  ];

  const values = [];
  const placeholders = [];
  let placeholderCounter = 1;

  batch.forEach((insertData) => {
    const currentPlaceholders = [];

    columns.forEach((column) => {
      const columnName = column.replace(/"/g, ""); // Remove quotes to access insertData keys
      values.push(
        insertData[columnName] !== undefined ? insertData[columnName] : null
      );
      currentPlaceholders.push(`$${placeholderCounter++}`);
    });

    placeholders.push(`(${currentPlaceholders.join(", ")})`);
  });

  const query = `
    INSERT INTO research (${columns.join(", ")})
    VALUES ${placeholders.join(", ")}
  `;

  try {
    const client = await pool.connect();
    // Execute the insertion query
    await client.query(query, values);
    client.release();
    console.log(`Batch of ${batch.length} records saved successfully.`);
  } catch (error) {
    console.error(`Error saving batch of records:`, error);
    throw error;
  }
}

// Function to get a random rating (implement this function)
function getRandomRating() {
  return Math.floor(Math.random() * 1000) + 1; // Example implementation, returns a rating between 1 and 5
}
function getRandomCountries(n = 4) {
  const randomCountries = [];
  while (randomCountries.length < n) {
    const randomIndex = Math.floor(Math.random() * countryList.length);
    const selectedCountry = countryList[randomIndex];
    if (!randomCountries.includes(selectedCountry)) {
      randomCountries.push(selectedCountry);
    }
  }
  return randomCountries;
}

const main = async () => {
  const state = readState(); // Read the last processed index from the state file
  console.log("Last processed index:", state.lastProcessedIndex);
  let nextPageToken = null;

  // Start processing from the last saved index + 1
  for (let i = state.lastProcessedIndex + 1; i < dataList.length; i++) {
    const { major_theme, category } = dataList[i];
    let hasMoreData = true; // Reset this for every major theme
    nextPageToken = null; // Reset nextPageToken for every major theme

    try {
      // Keep fetching until no more data (hasMoreData is false)
      while (hasMoreData) {
        const { adsData, pagetoken } = await adnovaAdsFetch(
          nextPageToken,
          major_theme
        );

        if (adsData && adsData.length > 0) {
          console.log(
            `Fetched ${adsData.length} ads for major theme: ${major_theme}`
          );

          const batch = adsData.map((ad) =>
            mapNewData(ad, category, major_theme)
          );
          // Write the batch to a JSON file
          writeBatchToFile(batch);
          console.log(`Saved ${batch.length} ads to the file`);
          if (batch.length > 0) {
            await saveBatchToDatabase(batch);
            console.log(
              `Saved ${batch.length} ads to the database for major theme: ${major_theme}`
            );
          }
          // Update nextPageToken and continue fetching if available
          nextPageToken = pagetoken;
          hasMoreData = nextPageToken ? true : false;
        } else {
          // No more data, move to the next major theme
          hasMoreData = false;
        }
      }

      // Save the last processed index in the state
      saveState(i);
    } catch (error) {
      console.error(
        `Error processing major theme: ${major_theme}, skipping to next.`,
        error.message
      );
    }
  }
};

main();
