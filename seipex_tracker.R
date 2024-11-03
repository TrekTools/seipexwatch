library(httr)
library(dplyr)
library(tibble)
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

head(seipex_timeseries)

# Execute the DELETE statement
#dbExecute(con, "DELETE FROM seipex_tokens;")

# Write the tokens table to the PostgreSQL database
dbWriteTable(con, "seipex_timeseries", seipex_timeseries, row.names = FALSE, overwrite = FALSE)
