# Escape Charlottesville - DS5220 Project 3

### Overview
This project uses the HERE Api to get the drive time between the School of Data Science (SDS) and the Interchange of Route 19 and Interstate 64. This allows for quick viewing of traffic conditions at the current time, recent trends, and weekend vs weekday averages across each hour. The drive time for both going north and south along that route is tracked, allowing for increased comparison. Both directions are around 280s (4m40s) overnight during off hours. Here are the Latitude and Longitudes used as part of the API call:

- SDS lat/long: 38.0405308,-78.5079088
- Interchange of 29 and 64 (S): 38.0223135,-78.5341943
- Interchange of 29 and 64 (N): 38.0195483,-78.5387543

### Data
Data is sampled every 20 minutes. This is the maximum possible while remaining within the free tier for the API. It is stored in a DynamoDB using the keys `route` (south or north) and `timestamp` (UTC datetime) while storing the data for the trip: `duration` (seconds to reach destination), `base_duration` (seconds for that trip with zero traffic), `distance` (meters traveled for the trip), and `delta` (the change is seconds of the duration since the last sample).

Metadata is stored in csv files in an S3 bucket that allows for use of Welford's Online Algorithm to get a constant update of the mean and standard deviation of the duration. This is aggregated into separate files for the North route and South route. Within each file, there is a row for each hour (0-23) with columns separating between weekdays and weekends.

### Chalice API Resources

- `/` : Displays the "about" information and lists the other resources
- `/current` : returns the most recent north and south drive times
- `/trend` : returns the average change in drive time over the past hour (based on 20-minute increments) and an estimation of the drive time 20 minutes in the future from the most recent datapoint based on that average change
- `/plot` : returns a plot of the past 24 hours of drive times for both north and south trips
- `/plot/weekday` : returns a plot of the average drive time for each hour aggregated across all weekdays (includes confidence interval based on rolling standard deviation)
- `/plot/weekend` : returns a plot of the average drive time for each hour aggregated across all weekend days (includes confidence interval based on rolling standard deviation)
