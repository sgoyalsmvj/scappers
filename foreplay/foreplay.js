import { v4 as uuidv4 } from "uuid";
import { countryList, countryToLanguage, dataList } from "../datalist.js";
import fs from "fs";
import pkg from "pg"; // Updated pg import to handle CommonJS module
import { awsConfig, s3Config, dbConfig } from "./variables.js";

const { Pool } = pkg;

const pool = new Pool(dbConfig);
const loggedUrls = new Set();
let access_token =
  "eyJhbGciOiJSUzI1NiIsImtpZCI6ImNjNWU0MTg0M2M1ZDUyZTY4ZWY1M2UyYmVjOTgxNDNkYTE0NDkwNWUiLCJ0eXAiOiJKV1QifQ.eyJuYW1lIjoiSkp0aG9tYXMiLCJpc3MiOiJodHRwczovL3NlY3VyZXRva2VuLmdvb2dsZS5jb20vYWRpc29uLWZvcmVwbGF5IiwiYXVkIjoiYWRpc29uLWZvcmVwbGF5IiwiYXV0aF90aW1lIjoxNzI1ODU1NDk0LCJ1c2VyX2lkIjoiZzJkZzE3VTZIb1dQN0V6MXdXcmozMDJ1WE5qMSIsInN1YiI6ImcyZGcxN1U2SG9XUDdFejF3V3JqMzAydVhOajEiLCJpYXQiOjE3MjU4NjcyMDQsImV4cCI6MTcyNTg3MDgwNCwiZW1haWwiOiJjb2dvbW9tMjU3QGphbmZhYi5jb20iLCJlbWFpbF92ZXJpZmllZCI6ZmFsc2UsImZpcmViYXNlIjp7ImlkZW50aXRpZXMiOnsiZW1haWwiOlsiY29nb21vbTI1N0BqYW5mYWIuY29tIl19LCJzaWduX2luX3Byb3ZpZGVyIjoicGFzc3dvcmQifX0.E20vYcXmHuCK6ruE_OndDyB9p6VqIdBnQ4Z6poVm78m7vA6y5pCjnmsb_DSPhVpUdlldQTPpzvUlhgQCvXBuvFENVO21YQOBlO6ouVsK_zCGig8-OEVbnn40HyVkg7nbZs4tX0Ls_RCHtQwVnkjrNyJg92jbBGslUdOPwd4c_GKTRYBGEVooB2hQc4LssjdpNLIGykkJT1-sO6hPMI4t6IL38tNPCFEW6-o7e9cHa1F23M-lgZlEdDEiZPPYL23KddWUE2cLOY1AWU89AOAeibgkZ-ZTyaCfXP1GZQtMqXwG-cDy_KZVYAP6uSkywybLQLnREGVNYvR3terkfccAcg";

// Function to read the state (last processed index)
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

// __dirname ko replicate karne ke liye
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Now define your file paths
const stateFile = path.join(__dirname, "state.json");
const outputFile = path.join(__dirname, "adsData.json");
const downloadFile = path.join(__dirname, "downloadfinal.txt"); // For downloadfinal.txt file

// Update your replaceUrls function accordingly
const replaceUrls = (url) => {
  const patterns = [
    "https://storage.googleapis.com/adison-foreplay.appspot.com/",
    "https://r2.foreplay.co/",
  ];
  const replacement = "https://d1br1uj2vallv9.cloudfront.net/graphics/";
  for (const pattern of patterns) {
    if (url && url.startsWith(pattern)) {
      const newUrl = url.replace(pattern, replacement);
      if (!loggedUrls.has(url)) {
        fs.appendFileSync(downloadFile, `${url}\n`); // Using downloadFile path here
        loggedUrls.add(url);
      }
      return newUrl;
    }
  }
  return url;
};

// Function to get random countries from the country list
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

// Function to refresh the access token
const refreshToken = async () => {
  const myHeaders = new Headers();
  myHeaders.append("accept", "*/*");
  myHeaders.append("accept-language", "en-US,en;q=0.9");
  myHeaders.append("content-type", "application/x-www-form-urlencoded");
  myHeaders.append("origin", "https://app.foreplay.co");
  myHeaders.append("priority", "u=1, i");
  myHeaders.append("referer", "https://app.foreplay.co/");
  myHeaders.append(
    "sec-ch-ua",
    '"Chromium";v="128", "Not;A=Brand";v="24", "Brave";v="128"'
  );
  myHeaders.append("sec-ch-ua-mobile", "?0");
  myHeaders.append("sec-ch-ua-platform", '"Windows"');
  myHeaders.append("sec-fetch-dest", "empty");
  myHeaders.append("sec-fetch-mode", "cors");
  myHeaders.append("sec-fetch-site", "cross-site");
  myHeaders.append("sec-gpc", "1");
  myHeaders.append(
    "user-agent",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
  );
  myHeaders.append("x-client-version", "Chrome/JsCore/8.10.1/FirebaseCore-web");

  const urlencoded = new URLSearchParams();
  urlencoded.append("grant_type", "refresh_token");
  urlencoded.append(
    "refresh_token",
    "AMf-vBweGPXEFfQcrRkzOIBefmVOnQxNzEO1eUCd2khQ9mJmdLb_Praf-v4czMRzzKPM85ehLNkCbnQsw3S8zertWnzibpEn6nZkd1HPou6mqpXktVLe14Dysrmrs_XjX3wCtpbljVg4qnK6NKTUvUxMh6oCt7tWNqzZ2vzw8cvU7SwY4F1vEtiN_2Vv0UO2CNT3HF2ag13jEewPLzjZp1hb3RZpXONUT7UAttbILKQRncSnBOj7bBU"
  );

  const requestOptions = {
    method: "POST",
    headers: myHeaders,
    body: urlencoded,
    redirect: "follow",
  };
  try {
    const response = await fetch(
      "https://securetoken.googleapis.com/v1/token?key=AIzaSyCIn3hB6C5qsx5L_a_V17n08eJ24MeqYDg",
      requestOptions
    );
    const data = await response.json();
    console.log("Refresh token response:", data);
    return { access_token: data.access_token }; // Return new access token
  } catch (error) {
    console.error("Error refreshing token:", error);
  }
};

// Function to fetch ads data from the API
const foreplay = async (textSearch, next) => {
  let url = `https://api.foreplay.co/ads/discovery?sort=relevance&textSearch=${textSearch}&next=${next}`;
  if (!next) {
    url = `https://api.foreplay.co/ads/discovery?sort=relevance&textSearch=${textSearch}`;
  }
  if (!textSearch) {
    url = `https://api.foreplay.co/ads/discovery?sort=relevance&textSearch=`;
  }
  try {
    const res = await fetch(url, {
      headers: {
        accept: "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        authorization: "Bearer " + access_token,
        "cache-control": "no-cache",
        pragma: "no-cache",
        priority: "u=1, i",
        "sec-ch-ua":
          '"Chromium";v="128", "Not;A=Brand";v="24", "Brave";v="128"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "sec-gpc": "1",
        Referer: "https://app.foreplay.co/",
        "Referrer-Policy": "strict-origin-when-cross-origin",
      },
      body: null,
      method: "GET",
    });
    const data = await res.json();

    if (data.message) {
      console.log("Refreshing token");
      const tokenData = await refreshToken();
      if (tokenData) {
        access_token = tokenData.access_token;
        return foreplay(textSearch, next); // Retry with new token
      }
    }
    console.log("Fetched data");
    return { adsdata: data.results, next: data.nextPage };
  } catch (error) {
    console.error("Error fetching data:", error);
  }
};

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
    await client.query(query, values);
    client.release();
    console.log(`Batch of ${batch.length} records saved successfully.`);
  } catch (error) {
    console.error(`Error saving batch of records:`, error);
    throw error;
  }
}

function getCountriesByLanguages(languages) {
  const languageToCountries = {};

  // Iterate over the list of languages
  languages.forEach((language) => {
    // Initialize array to store countries where this language is spoken
    const countriesForLanguage = [];

    // Traverse the countryToLanguage object to find matching countries
    for (const country in countryToLanguage) {
      if (countryToLanguage[country].includes(language)) {
        countriesForLanguage.push(country);
      }
    }

    // Map language to its list of countries
    languageToCountries[language] = countriesForLanguage;
  });
  console.log(languageToCountries);
  return languageToCountries;
}

// Function to map new data to a specific format
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
  return {
    id: uuidv4(),
    family: "S_FPY",
    objectID: newData.ad_id?.toString() || null,
    brand: validateAndStringifyJson({
      id: newData.brandId || null,
      page_id: newData.board_ids?.[0] || null,
      name: newData.name || null,
      avatar_url: newData.avatar || null,
      website_url: newData.link_url || null,
    }),
    ad_external_link: newData.link_url || null,
    country: formatArrayForPostgres(getRandomCountries(4)) || null,
    ad: validateAndStringifyJson({
      id: newData.ad_id?.toString() || null,
      display_format: newData.display_format || null,
      body: newData.description || null,
      title: newData.name || null,
    }),
    ad_status: newData.live ? "active" : "inactive",
    ad_start_date: newData.startedRunning
      ? new Date(newData.startedRunning).toISOString()
      : null,
    ad_end_date: null,
    ad_platforms: formatArrayForPostgres(newData.publisher_platform) || null,
    ad_display_format: newData.display_format
      ? newData.display_format.toLowerCase()
      : null,
    analysis: null,
    s3_image_url: replaceUrls(newData.image) || null,
    s3_video_url: replaceUrls(newData.video) || null,
    ad_image: null,
    ad_video: null,
    category: category || null,
    language: newData.languages?.[0] || null,
    theme: null,
    created_at: newData.createdAt
      ? new Date(newData.createdAt).toISOString()
      : null,
    updated_at: new Date().toISOString(),
    likes: newData.likes || null,
    preview_image_url: null,
    title: newData.headline || null,
    video_len: null,
    saved_details: newData.timestampedTranscription
      ? JSON.stringify(newData.timestampedTranscription)
      : null,
    ad_categories: null,
    marketing_goals: null,
    rating: getRandomRating(), // Make sure to implement getRandomRating function
    report: null,
    major_theme: major_theme || null,
    sub_theme: null,
    spend: null,
    impression: null,
    focus_area: null,
    video_len: null,
    ad_body: newData.description || null,
    ad_body_tsv: null, // Remove to_tsvector for now
    brand_name: newData.name || null,
  };
}

// Function to get a random rating (implement this function)
function getRandomRating() {
  return Math.floor(Math.random() * 1000) + 1; // Example implementation, returns a rating between 1 and 5
}

// Main function to process the data
const main = async () => {
  const state = readState();
  console.log(state); // Read the last processed index from the state file
  let nextPage = null;

  for (let i = state.lastProcessedIndex + 1; i < dataList.length; i++) {
    const { major_theme, category } = dataList[i];
    let hasMoreData = true;

    try {
      while (hasMoreData) {
        const { adsdata, next } = await foreplay(major_theme, nextPage);

        if (adsdata && adsdata.length > 0) {
          console.log(
            `Fetched ${adsdata.length} ads for major theme: ${major_theme}`
          );

          const batch = adsdata.map((ad) =>
            mapNewData(ad, category, major_theme)
          );
          console.log(batch);
          // Write the batch to a JSON file
          writeBatchToFile(batch);
          console.log(`Saved ${batch.length} ads to the file`);
          if (batch.length > 0) {
            await saveBatchToDatabase(batch);
            console.log(
              `Saved ${batch.length} ads to the database for major theme: ${major_theme}`
            );
          }
          console.log("Next page:", next);
          nextPage = next; // Update nextPage for pagination
          hasMoreData = nextPage ? true : false; // Check if more data is available
        } else {
          hasMoreData = false;
        }
      }

      saveState(i); // Save current state
    } catch (error) {
      console.error(
        `Error processing major theme: ${major_theme}, skipping to next.`,
        error.message
      );
    }
  }
};

// Run the main function
main();
