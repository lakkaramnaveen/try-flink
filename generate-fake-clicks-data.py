"""Generate youtube-clicks.csv: one simulated day of clicks with the columns videoid and timestamp.

Design:
To simulate human activity, we model two layers:

 - Video Popularity (Zipf's Law): A tiny fraction of videos (the "viral" hits) get the vast majority of clicks, while a massive tail of videos gets almost none.

 - Time-of-Day Traffic (Sine Wave + Noise): Clicks peak during the evening and bottom out in the early morning hours, with each click sampled at random so the curve is noisy like real traffic.

"""

import csv
from datetime import datetime, timedelta

import numpy as np

# Configuration
NUM_EVENTS = 100000
NUM_VIDEOS = 500  # Total pool of video IDs
ZIPF_ALPHA = 1.2  # Higher = more concentrated on top videos
SEED = 2026  # Fixed so regenerating gives the same file.  Change it for a different day of traffic.
OUTPUT_FILE = 'youtube-clicks.csv'

np.random.seed(SEED)

# 1. Generate Video IDs based on Zipf distribution
video_pool = [f"vid_{i:04d}" for i in range(1, NUM_VIDEOS + 1)]
weights = 1.0 / (np.arange(1, NUM_VIDEOS + 1) ** ZIPF_ALPHA)
weights /= weights.sum()  # Normalize probabilities
chosen_videos = np.random.choice(video_pool, size=NUM_EVENTS, p=weights)

# 2. Generate timestamps with a diurnal curve
start_time = datetime(2026, 9, 23, 0, 0, 0)
timestamps = []

# Distribute events non-uniformly throughout a 24-hour day
while len(timestamps) < NUM_EVENTS:
    # Pick a random second in the day
    random_second = np.random.randint(0, 86400)
    current_dt = start_time + timedelta(seconds=random_second)
    hour = current_dt.hour
    
    # Probability curve: Peak at 8 PM (20:00), low at 4 AM (04:00)
    # Scaled between 0.2 and 1.0
    activity_prob = 0.6 + 0.4 * np.sin(2 * np.pi * (hour - 14) / 24)
    
    if np.random.rand() < activity_prob:
        timestamps.append(current_dt.strftime("%Y-%m-%d %H:%M:%S"))

# Sort timestamps so the stream moves forward chronologically
timestamps.sort()

# 3. Write out to a simple CSV file
with open(OUTPUT_FILE, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['videoid', 'timestamp'])
    for vid, ts in zip(chosen_videos, timestamps):
        writer.writerow([vid, ts])

print(f"{OUTPUT_FILE} successfully generated!")

