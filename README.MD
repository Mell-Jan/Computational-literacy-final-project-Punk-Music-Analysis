Hello!
This Archive includes all the information you will need for the project: The use of profanity in punk lyrics through the years.

The Archive is only missing the data sets. 
If you need those, please go to https://huggingface.co/datasets/brunokreiner/genius-lyrics for the lyrics.
And the Music Brainz Dataset can be found here https://data.musicbrainz.org/pub/musicbrainz/data/fullexport/

You will need to get the following:
The Musicbrainz DUMP exists in two parts: first is mbdump.tar.bz2, in there you will find all general information that MusicBrainz provides under Creative Commons Zero (CC0).
The second DUMP is mbdump-derived.tar.bz2 and includes such information as the tags for each track. This was needed to map them accordingly to the Punk genre, and it is under the Attribution-NonCommercial-ShareAlike 3.0.

The JSON file profanity_with_info.json contains the information for all profanity-linked information, such as the words themselves, the severity ranking, and tags. It is sourced from https://github.com/dsojevic/profanity-list/tree/main.

Otherwise, you will find the necessary Python scripts to recreate the database inside the folder Database Approach.
You will simply need to adjust the paths for the files inside the Python functions, as well as the path for the database.

There is one SQL file. When I was analysing the data, I switched to a SQL editor so I could check the data directly and didn't have to go the extra mile and do everything in Python.
I used DBeaver for that, so if a tool is needed, I suggest that.

There is also the API approach, which is just interesting if you want to see how the MusicBrainz database is accessible through their API and see my approach to do it automatically.
