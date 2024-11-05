library(httr)
library(dplyr)
library(tibble)
library(purrr)
library(RPostgres)
library(lubridate)

q <- GET("https://seipex.fi/api/tokens")
api_data <- content(q,'parsed')

str(api_data$tokens[[1]])

all_data <- list()
# Iterate through each item in api_data$tokens[[i]]

# Iterate through each token in api_data$tokens
for (token in api_data$tokens) {
  # Flatten the current token data and store it as a named list
  all_data <- append(all_data, list(tibble(
    address = token$address,
    name = token$name,
    symbol = token$symbol,
    decimals = token$decimals,
    totalSupply = token$totalSupply,
    migrated = token$migrated,
    social_image_uri = token$socialData$image_uri,
    social_description = token$socialData$description,
    social_website = token$socialData$website,
    social_twitter = token$socialData$twitter,
    social_telegram = token$socialData$telegram,
    social_discord = token$socialData$discord,
    marketCap_sei = token$marketCap$sei,
    marketCap_usd = token$marketCap$usd,
    liquidity_sei = token$liquidity$sei,
    liquidity_usd = token$liquidity$usd,
    liquidity_starting = token$liquidity$starting,
    progress = token$progress,
    creator = token$creator,
    creationTimestamp = token$creationTimestamp,
    holderCount = token$holderCount,
    top10HoldersPercentage = token$top10HoldersPercentage
  )))
}

# Combine all rows into a single dataframe
df <- bind_rows(all_data)

# Print the resulting dataframe

seipex_tokens <- as.data.frame(df)
head(seipex_tokens)

con <- dbConnect(
  RPostgres::Postgres(),
  host = "sei-nft.c7y6wya64l18.eu-west-1.rds.amazonaws.com",
  port = 5432,
  dbname = "sei_nfts",
  user = "nftadmin",
  password = Sys.getenv("DB_PASSWORD")
)

# Execute the DELETE statement
dbExecute(con, "DELETE FROM seipex_tokens;")

# Write the tokens table to the PostgreSQL database
dbWriteTable(con, "seipex_tokens", seipex_tokens, row.names = FALSE, overwrite = TRUE)

seipex_tokens$rounded_time <- round_date(Sys.time(), unit = "hour")
head(seipex_tokens)

str(seipex_tokens)

seipex_timeseries <- seipex_tokens %>%
  select(
    address,
    name,
    symbol,
    marketCap_sei,
    marketCap_usd,
    liquidity_sei,
    liquidity_usd,
    holderCount,
    top10HoldersPercentage,
    rounded_time
  )

# Execute the DELETE statement
#dbExecute(con, "DELETE FROM seipex_tokens;")

# Write the tokens table to the PostgreSQL database
dbWriteTable(con, "seipex_timeseries", seipex_timeseries, row.names = FALSE, overwrite = FALSE, append = TRUE)

pallet1 <- paste0("https://rest.sei-apis.com/cosmwasm/wasm/v1/contract/sei152u2u0lqc27428cuf8dx48k8saua74m6nql5kgvsu4rfeqm547rsnhy4y9/smart/ewogICJ2ZXJpZmllZF9uZnRzIjogewogICAgImxpbWl0IjogMTAwCiAgfQp9")
pallet2 <- paste0("https://rest.sei-apis.com/cosmwasm/wasm/v1/contract/sei152u2u0lqc27428cuf8dx48k8saua74m6nql5kgvsu4rfeqm547rsnhy4y9/smart/ewogICJ2ZXJpZmllZF9uZnRzIjogewogICAgImxpbWl0IjogMTAwLAogICAgInN0YXJ0X2FmdGVyIjogInNlaTE3dHZqOXYyM3cyOXJnZGV3cXBkN2RkMDNwamcybXA0NDNxNzZrZXg3c2NqcTJtMG0zcHJxcDJrczZjIgogIH0KfQ==")
pallet3 <- paste0("https://rest.sei-apis.com/cosmwasm/wasm/v1/contract/sei152u2u0lqc27428cuf8dx48k8saua74m6nql5kgvsu4rfeqm547rsnhy4y9/smart/ewogICJ2ZXJpZmllZF9uZnRzIjogewogICAgImxpbWl0IjogMTAwLAogICAgInN0YXJ0X2FmdGVyIjogInNlaTFldXZkdG5qbmR4ZG50dTJwcTRqanB3enhqZGdzbGZ3ZXhtbGFodjNycXZ6d20yaHlnZnZxOWs2NnY0IgogIH0KfQ==")
pallet4 <- paste0("https://rest.sei-apis.com/cosmwasm/wasm/v1/contract/sei152u2u0lqc27428cuf8dx48k8saua74m6nql5kgvsu4rfeqm547rsnhy4y9/smart/ewogICJ2ZXJpZmllZF9uZnRzIjogewogICAgImxpbWl0IjogMTAwLAogICAgInN0YXJ0X2FmdGVyIjogInNlaTFtZmp5M2x6YXZ0cHEwNDBmc3BhY2FueWg1NnNranUwOXhnOGswZmE1dmpuejNwYWM2d2ZzaDduYWxhIgogIH0KfQ==")
pallet5 <- paste0("https://rest.sei-apis.com/cosmwasm/wasm/v1/contract/sei152u2u0lqc27428cuf8dx48k8saua74m6nql5kgvsu4rfeqm547rsnhy4y9/smart/ewogICJ2ZXJpZmllZF9uZnRzIjogewogICAgImxpbWl0IjogMTAwLAogICAgInN0YXJ0X2FmdGVyIjogInNlaTF0OHJ2NDJxZ3k5bjd2NmY0MGtkbHB1NTh0ZGU1bWZwZ2t5Y3g3Nzd3cW5ybHg1ZGR0NnVxZTYwdHJmIgogIH0KfQ==")
pallet6 <- paste0("https://rest.sei-apis.com/cosmwasm/wasm/v1/contract/sei152u2u0lqc27428cuf8dx48k8saua74m6nql5kgvsu4rfeqm547rsnhy4y9/smart/ewogICJ2ZXJpZmllZF9uZnRzIjogewogICAgImxpbWl0IjogMTAwLAogICAgInN0YXJ0X2FmdGVyIjogInNlaTF6cjcyeHJ4aHFxcDJjaHRucmFsNDV0cHp2cmgyZmRyNXc5bTk2ajN3Y20wMzY1cWh4Z3JxZDlkeHpxIgogIH0KfQ==")

pallet1 <- GET(pallet1)
pallet1 <- content(pallet1,'parsed')
pallet2 <- GET(pallet2)
pallet2 <- content(pallet2,'parsed')
pallet3 <- GET(pallet3)
pallet3 <- content(pallet3,'parsed')
pallet4 <- GET(pallet4)
pallet4 <- content(pallet4,'parsed')
pallet5 <- GET(pallet5)
pallet5 <- content(pallet5,'parsed')
pallet6 <- GET(pallet6)
pallet6 <- content(pallet6,'parsed')

# Assuming the lists are named pallet1$data, pallet2$data, ..., pallet6$data
# Combine the lists into a single list
combined_list <- c(pallet1$data, pallet2$data, pallet3$data, pallet4$data, pallet5$data, pallet6$data)

# Convert the combined list to a data frame with more robust extraction
pallet <- map_df(combined_list, ~ {
  twitter_link <- NA
  discord_link <- NA
  
  # Check if the socials field exists and is a non-empty list
  if (!is.null(.x$socials) && length(.x$socials) > 0) {
    for (social in .x$socials) {
      if ("twitter" %in% names(social)) {
        twitter_link <- social$twitter
      }
      if ("discord" %in% names(social)) {
        discord_link <- social$discord
      }
    }
  }
  
  tibble(
    address = .x$address,
    minter = .x$minter,
    name = .x$name,
    symbol = .x$symbol,
    description = .x$description,
    twitter = twitter_link,
    discord = discord_link
  )
})

pallet <- pallet %>% as.data.frame()

# Placeholder list of addresses, assuming it's named `pallet$address`
# Replace `pallet$address` with the actual data source
addresses_list <- pallet$address

# Function to get and parse data for each address
get_address_data <- function(address) {
  tryCatch({
    # Construct the URL and make the GET request
    slug_getter <- paste0("https://api.pallet.exchange/api/v2/nfts/", address, "/details")
    response <- GET(slug_getter)
    
    # Check if the response status is successful
    if (response$status_code != 200) {
      warning("Non-200 status code for address: ", address)
      return(NULL)
    }
    
    # Parse the response content
    address_data <- content(response, 'parsed')
    
    # Check if the response is valid
    if (is.null(address_data) || !is.list(address_data)) {
      warning("Invalid or unexpected response structure for address: ", address)
      return(NULL)
    }
    
    # Extract social media links
    twitter_link <- NA
    discord_link <- NA
    website_link <- NA
    
    if (!is.null(address_data$socials) && is.list(address_data$socials)) {
      for (social in address_data$socials) {
        if (is.list(social)) {
          if ("twitter" %in% names(social)) {
            twitter_link <- social$twitter
          }
          if ("discord" %in% names(social)) {
            discord_link <- social$discord
          }
          if ("website" %in% names(social)) {
            website_link <- social$website
          }
        }
      }
    }
    
    # Create a tibble with extracted data
    tibble(
      sei_address = if (!is.null(address_data$sei_address)) address_data$sei_address else NA,
      evm_address = if (!is.null(address_data$evm_address)) address_data$evm_address else NA,
      creator = if (!is.null(address_data$creator)) address_data$creator else NA,
      chain_id = if (!is.null(address_data$chain_id)) address_data$chain_id else NA,
      creator_domain = if (!is.null(address_data$creator_info$domain)) address_data$creator_info$domain else NA,
      creator_pfp = if (!is.null(address_data$creator_info$pfp)) address_data$creator_info$pfp else NA,
      creator_sei_address = if (!is.null(address_data$creator_info$sei_address)) address_data$creator_info$sei_address else NA,
      creator_evm_address = if (!is.null(address_data$creator_info$evm_address)) address_data$creator_info$evm_address else NA,
      name = if (!is.null(address_data$name)) address_data$name else NA,
      slug = if (!is.null(address_data$slug)) address_data$slug else NA,
      symbol = if (!is.null(address_data$symbol)) address_data$symbol else NA,
      description = if (!is.null(address_data$description)) address_data$description else NA,
      pfp = if (!is.null(address_data$pfp)) address_data$pfp else NA,
      banner = if (!is.null(address_data$banner)) address_data$banner else NA,
      twitter = twitter_link,
      discord = discord_link,
      website = website_link,
      supply = if (!is.null(address_data$supply)) address_data$supply else NA,
      owners = if (!is.null(address_data$owners)) address_data$owners else NA,
      auction_count = if (!is.null(address_data$auction_count)) address_data$auction_count else NA,
      floor = if (!is.null(address_data$floor)) address_data$floor else NA,
      volume = if (!is.null(address_data$volume)) address_data$volume else NA,
      num_sales_24hr = if (!is.null(address_data$num_sales_24hr)) address_data$num_sales_24hr else NA,
      volume_24hr = if (!is.null(address_data$volume_24hr)) address_data$volume_24hr else NA
    )
  }, error = function(e) {
    message("Error encountered for address: ", address, " - ", e$message)
    return(NULL)
  })
}

# Initialize an empty list to collect results
results_list <- list()

# Iterate over each address in the addresses_list
for (i in seq_along(addresses_list)) {
  address <- addresses_list[[i]]
  result <- get_address_data(address)
  
  # Only append non-NULL results
  if (!is.null(result)) {
    results_list[[length(results_list) + 1]] <- result
  }
  
  Sys.sleep(0.15)  # Add delay for rate limiting
}

# Combine the list of tibbles into a single data frame
pallet_data <- do.call(rbind, results_list)

# Convert to a data frame if needed
pallet_data <- as.data.frame(pallet_data)

pallet_data$rounded_time <- round_date(Sys.time(), unit = "hour")

max_record <- dbGetQuery(con, "SELECT current_max FROM max_record;")

pallet_timeseries <- pallet_data %>%
  select(
    sei_address,
    evm_address,
    name,
    slug,
    supply,
    owners,
    auction_count,
    floor,
    volume,
    rounded_time
  )

pallet_timeseries$record <- max_record$current_max

# Append the contents of pallet_new to the PostgreSQL table seimap
dbWriteTable(con, "pallet_timeseries", pallet_timeseries, row.names = FALSE, append = TRUE)

# DELETE
dbExecute(con, "DELETE FROM max_record;")
# INSERT
dbExecute(con, "INSERT INTO max_record (current_max) SELECT max(record)+1 FROM pallet_timeseries;")

dbExecute(con, "SELECT current_max FROM max_record;")

dbExecute(con, "DELETE FROM pallet_time_comparison;")

dbExecute(con, "create table pallet_time_comparison as
                (
                  WITH latest_data AS (
                      SELECT *
                      FROM pallet_timeseries
                      WHERE record = (SELECT current_max FROM max_record)
                  ),
                  hour_data AS (
                      SELECT *
                      FROM pallet_timeseries
                      WHERE record = (SELECT max(record) FROM pallet_timeseries WHERE record < (SELECT current_max FROM max_record))
                  ),
                  day_data AS (
                      SELECT *
                      FROM pallet_timeseries
                      WHERE record = (SELECT max(record) FROM pallet_timeseries WHERE record < (SELECT current_max - 23 FROM max_record))
                  ),
                  week_data AS (
                      SELECT *
                      FROM pallet_timeseries
                      WHERE record = (SELECT max(record) FROM pallet_timeseries WHERE record < (SELECT current_max - 167 FROM max_record))
                  )
                  SELECT 
                      l.slug,
                  
                      l.owners AS current_owners_1h,
                      h.owners AS previous_owners_1h,
                      l.owners - h.owners AS owners_diff_1h,
                      CASE WHEN h.owners != 0 THEN ((l.owners - h.owners) / h.owners::float) * 100 ELSE NULL END AS owners_percent_diff_1h,
                  
                      l.auction_count AS current_auction_count_1h,
                      h.auction_count AS previous_auction_count_1h,
                      l.auction_count - h.auction_count AS auction_count_diff_1h,
                      CASE WHEN h.auction_count != 0 THEN ((l.auction_count - h.auction_count) / h.auction_count::float) * 100 ELSE NULL END AS auction_count_percent_diff_1h,
                  
                      l.floor AS current_floor_1h,
                      h.floor AS previous_floor_1h,
                      l.floor - h.floor AS floor_diff_1h,
                      CASE WHEN h.floor != 0 THEN ((l.floor - h.floor) / h.floor::float) * 100 ELSE NULL END AS floor_percent_diff_1h,
                  
                      l.volume AS current_volume_1h,
                      h.volume AS previous_volume_1h,
                      l.volume - h.volume AS volume_diff_1h,
                      CASE WHEN h.volume != 0 THEN ((l.volume - h.volume) / h.volume::float) * 100 ELSE NULL END AS volume_percent_diff_1h
                  --	 ,
                  --    -- 1-day comparison
                  --    d.owners AS previous_owners_1d,
                  --    l.owners - d.owners AS owners_diff_1d,
                  --    CASE WHEN d.owners != 0 THEN ((l.owners - d.owners) / d.owners::float) * 100 ELSE NULL END AS owners_percent_diff_1d,
                  --
                  --    d.auction_count AS previous_auction_count_1d,
                  --    l.auction_count - d.auction_count AS auction_count_diff_1d,
                  --    CASE WHEN d.auction_count != 0 THEN ((l.auction_count - d.auction_count) / d.auction_count::float) * 100 ELSE NULL END AS auction_count_percent_diff_1d,
                  --
                  --    d.floor AS previous_floor_1d,
                  --    l.floor - d.floor AS floor_diff_1d,
                  --    CASE WHEN d.floor != 0 THEN ((l.floor - d.floor) / d.floor::float) * 100 ELSE NULL END AS floor_percent_diff_1d,
                  --
                  --    d.volume AS previous_volume_1d,
                  --    l.volume - d.volume AS volume_diff_1d,
                  --    CASE WHEN d.volume != 0 THEN ((l.volume - d.volume) / d.volume::float) * 100 ELSE NULL END AS volume_percent_diff_1d,
                  --
                  --     1-week comparison
                  --    w.owners AS previous_owners_1w,
                  --    l.owners - w.owners AS owners_diff_1w,
                  --    CASE WHEN w.owners != 0 THEN ((l.owners - w.owners) / w.owners::float) * 100 ELSE NULL END AS owners_percent_diff_1w,
                  --
                  --    w.auction_count AS previous_auction_count_1w,
                  --    l.auction_count - w.auction_count AS auction_count_diff_1w,
                  --    CASE WHEN w.auction_count != 0 THEN ((l.auction_count - w.auction_count) / w.auction_count::float) * 100 ELSE NULL END AS auction_count_percent_diff_1w,
                  --
                  --    w.floor AS previous_floor_1w,
                  --    l.floor - w.floor AS floor_diff_1w,
                  --    CASE WHEN w.floor != 0 THEN ((l.floor - w.floor) / w.floor::float) * 100 ELSE NULL END AS floor_percent_diff_1w,
                  --
                  --    w.volume AS previous_volume_1w,
                  --    l.volume - w.volume AS volume_diff_1w,
                  --    CASE WHEN w.volume != 0 THEN ((l.volume - w.volume) / w.volume::float) * 100 ELSE NULL END AS volume_percent_diff_1w
                  
                  FROM latest_data l
                  JOIN hour_data h ON l.slug = h.slug
                  --JOIN day_data d ON l.slug = d.slug
                  --JOIN week_data w ON l.slug = w.slug
                  ORDER BY volume_diff_1h DESC
                );")