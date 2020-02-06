# video-detect
detecting videos in harvard websites


[VideoDetect.py](VideoDetect.py) is the main script you're probably interested in. Used to find all embedded and hyperlinked videos. You can connect to the vpal-link-data s3 bucket to access person_resource, or use a local version of person_resource.

The output from VideoDetect.py can be found in [data/entity_videos.csv](data/entity_videos.csv). It has the following fields:

- **entity_id**: entity id as defined from the link database
- **url**: the url that was searched
- **total_videos**: total number of videos found on the website associated with `url`
- **num_embedded**: number of embedded videos found
- **embedded_html**: list of snippets of the html embed code associated with each embedded video
- **num_hyperlinked**: number of hyperlinked videos found
- **hyperlinked_html**: list of snippets of the html hyperlinked code associated with each embedded video

For doing the analytics, I use 2 different scripts:
[urls.py](urls.py) extracts the actual urls to prep for the [youtube analytics notebook](youtubeAnalytics.ipynb). The youtube analytics notebook uses the youtube API to extract meta data about each video.

for more info, check out this [summary](summary.nb.html).
