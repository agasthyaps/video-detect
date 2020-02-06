# video-detect
detecting videos in harvard websites


[VideoDetect.py](VideoDetect.py) is used to find all embedded and hyperlinked videos. You can connect to the vpal-link-data s3 bucket to access person_resource, or use a local version of person_resource.

The output from VideoDetect.py can be found in [data/entity_videos.csv](data/entity_videos.csv)


[urls.py](urls.py) extracts the actual urls to prep for the [youtube analytics script](youtubeAnalytics.ipynb).
