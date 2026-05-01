


SDS lat/long: 38.0405308,-78.5079088
Intersection of 29 and 64 (S): 38.0223135,-78.5341943 (283)
Intersection of 29 and 64 (N): 38.0195483,-78.5387543 (279)

both around 280s (4m40s) at midnight

deploy part 1:
cd ingestion/

# first time only
sam build
sam deploy --guided      # walks you through region, stack name, etc.
                         # saves settings to samconfig.toml

# subsequent deploys
sam build && sam deploy
