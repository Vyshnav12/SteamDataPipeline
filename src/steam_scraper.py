########################################################################################################################
# Copyright (c) Martin Bustos @FronkonGames <fronkongames@gmail.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
# documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies or substantial portions of
# the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
# WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

# Modifications by Vyshnav Varma <vyshnavvarma@gmail.com>
# Date: 01/09/2024
#
# 1. Performance Enhancements:
#    - Implemented chunk-based JSON handling to mitigate memory issues on EC2 instances.
#    - Introduced set-based lookups for AppIDs, improving lookup performance.
#    - Optimized the `Scraper` function for better performance and error handling.
#
# 2. Data Management:
#    - Added metadata index functions to reduce redundant API calls by caching processed AppIDs.
#    - Implemented manifest tracking to manage chunk file data before combination.
#    - Modified `ParseSteamGame` function to safely handle missing fields and exclude certain fields from processing.
#
# 3. AWS Integration:
#    - Replaced saveJSON/loadJSON with functions to interact directly with Amazon S3.
#
# 4. Code Structure and Organization:
#    - Improved overall code readability.
#    - Split functions into separate files and reorganized imports for better maintainability.
#
# 5. Logging and Error Handling:
#    - Enhanced logging system for better tracking of scraping progress and error reporting.
#
# 6. Removed Deprecated Functions and Parser Arguments:
#    - Removed UpdateFromJSON and unused parser arguments from the script.
#
# These modifications aim to improve the scraper's efficiency, reliability, and maintainability,
# particularly when deployed on AWS infrastructure.
########################################################################################################################

__author__ = "Martin Bustos, Vyshnav Varma"
__copyright__ = "Copyright 2022, Martin Bustos; 2024, Vyshnav Varma"
__license__ = "MIT"
__version__ = "1.0"


import sys
import json
import time
import argparse
import random
import datetime as dt
import config

from api import SteamRequest, SteamSpyRequest, DoRequest, ParseSteamGame
from utils import load_from_s3, save_to_s3, ProgressLog, Log, save_chunk_to_s3, merge_chunks, load_metadata_index, save_metadata_index, update_metadata_index

def get_app_list(bucket_name, args):
    try:
        apps = load_from_s3(bucket_name, config.APPLIST_FILE)
        if apps is None:
            raise FileNotFoundError
        Log(config.INFO, f'List with {len(apps)} games loaded from S3')
    except (FileNotFoundError, json.JSONDecodeError):
        Log(config.INFO, 'Requesting list of games from Steam')
        response = DoRequest('http://api.steampowered.com/ISteamApps/GetAppList/v2/')
        if response:
            time.sleep(args.sleep)
            data = response.json()
            apps = [str(x["appid"]) for x in data['applist']['apps']]
            save_to_s3(bucket_name, config.APPLIST_FILE, apps)
            Log(config.INFO, f'List with {len(apps)} games saved to S3.')
    return apps

def process_game(appID, args, notreleased_set, discarded_set, successRequestCount, errorRequestCount):
    app = SteamRequest(appID, min(4, args.sleep), successRequestCount, errorRequestCount, args.retries)
    if not app:
        return None, 'discarded'

    game = ParseSteamGame(app)
    if game['release_date'] == '':
        return None, 'not_released'

    if args.steamspy:
        extra = SteamSpyRequest(appID, min(4, args.sleep), successRequestCount, errorRequestCount, args.retries)
        if extra:
            game.update({
                'user_score': extra.get('userscore', 0),
                'score_rank': extra.get('score_rank', ""),
                'positive': extra.get('positive', 0),
                'negative': extra.get('negative', 0),
                'estimated_owners': extra.get('owners', "0 - 0").replace(',', '').replace('..', '-'),
                'average_playtime_forever': extra.get('average_forever', 0),
                'average_playtime_2weeks': extra.get('average_2weeks', 0),
                'median_playtime_forever': extra.get('median_forever', 0),
                'median_playtime_2weeks': extra.get('median_2weeks', 0),
                'peak_ccu': extra.get('ccu', 0),
                'tags': extra.get('tags', [])
            })
        else:
            game.update({
                'user_score': 0, 'score_rank': "", 'positive': 0, 'negative': 0,
                'estimated_owners': "0 - 0", 'average_playtime_forever': 0,
                'average_playtime_2weeks': 0, 'median_playtime_forever': 0,
                'median_playtime_2weeks': 0, 'peak_ccu': 0, 'tags': []
            })

    return game, 'added'

def save_progress(bucket_name, args, notreleased_set, discarded_set, gamesNotReleased, gamesdiscarded):
    if args.autosave > 0:
        if gamesNotReleased % args.autosave == 0:
            save_to_s3(bucket_name, config.NOTRELEASED_FILE, list(notreleased_set))
        if gamesdiscarded % args.autosave == 0:
            save_to_s3(bucket_name, config.DISCARDED_FILE, list(discarded_set))

def Scraper(dataset, notreleased, discarded, args, appIDs=None):
    bucket_name = 'testbucketx11'
    metadata = load_metadata_index(bucket_name)
    apps = appIDs or get_app_list(bucket_name, args)
    
    notreleased_set, discarded_set = set(notreleased), set(discarded)
    gamesAdded, gamesNotReleased, gamesdiscarded = 0, 0, 0
    successRequestCount, errorRequestCount = 0, 0

    random.shuffle(apps)
    total = len(apps) - len(discarded_set) - len(notreleased_set) - len(metadata)
    count, chunk_size = 0, 10000
    chunk, manifest = {}, load_from_s3(bucket_name, 'manifest.json') or {'chunks': []}
    start_time = dt.datetime.now()

    try:
        for appID in apps:
            if appID not in metadata and appID not in discarded_set:
                if args.released and appID in notreleased_set:
                    continue

                game, status = process_game(appID, args, notreleased_set, discarded_set, successRequestCount, errorRequestCount)

                if status == 'added':
                    chunk[appID] = game
                    gamesAdded += 1
                    count += 1
                    ProgressLog('Scraping', count, total, start_time)

                    if appID in notreleased_set:
                        notreleased_set.remove(appID)

                    if len(chunk) >= chunk_size:
                        manifest = save_chunk_to_s3(bucket_name, chunk, manifest)
                        metadata = update_metadata_index(metadata, chunk)
                        Log(config.INFO, f'Updated metadata index with chunk. Current metadata size: {len(metadata)}')
                        chunk.clear()
                elif status == 'not_released':
                    if appID not in notreleased_set:
                        notreleased_set.add(appID)
                        gamesNotReleased += 1
                elif status == 'discarded':
                    discarded_set.add(appID)
                    gamesdiscarded += 1

                save_progress(bucket_name, args, notreleased_set, discarded_set, gamesNotReleased, gamesdiscarded)
                time.sleep(args.sleep if random.random() > 0.1 else args.sleep * 2.0)

    except (KeyboardInterrupt, SystemExit):
        Log(config.INFO, 'Scraping interrupted. Saving current progress...')
        if chunk:  # Save the incomplete chunk
            manifest = save_chunk_to_s3(bucket_name, chunk, manifest)
            metadata = update_metadata_index(metadata, chunk)
        save_to_s3(bucket_name, config.DISCARDED_FILE, list(discarded_set))
        save_to_s3(bucket_name, config.NOTRELEASED_FILE, list(notreleased_set))
        save_metadata_index(bucket_name, metadata)
        save_to_s3(bucket_name, 'manifest.json', manifest)
        raise

    # Save remaining data and finalize
    if chunk:
        manifest = save_chunk_to_s3(bucket_name, chunk, manifest)
        metadata = update_metadata_index(metadata, chunk)

    ProgressLog('Scraping', total, total, start_time)
    print('\r')
    Log(config.INFO, f'Scrape completed: {gamesAdded} new games added, {gamesNotReleased} not released, {gamesdiscarded} discarded')
    save_to_s3(bucket_name, config.DISCARDED_FILE, list(discarded_set))
    save_to_s3(bucket_name, config.NOTRELEASED_FILE, list(notreleased_set))
    save_metadata_index(bucket_name, metadata)
    merge_chunks(bucket_name, config.UPDATE_OUTFILE)

if __name__ == "__main__":
    Log(config.INFO, f'Steam Games Scraper {__version__} by {__author__}')
  
    parser = argparse.ArgumentParser(description='Steam games scraper.')
    parser.add_argument('-s', '--sleep',    type=float, default=config.DEFAULT_SLEEP,    help='Waiting time between requests')
    parser.add_argument('-r', '--retries',  type=int,   default=config.DEFAULT_RETRIES,  help='Number of retries (0 to always retry)')
    parser.add_argument('-a', '--autosave', type=int,   default=config.DEFAULT_AUTOSAVE, help='Record the data every number of new entries (0 to deactivate)')
    parser.add_argument('-d', '--released', type=bool,  default=True,             help='If it is on the list of not yet released, no information is requested')
    parser.add_argument('-p', '--steamspy', type=bool,  default=True,             help='Add SteamSpy info')
    args = parser.parse_args()
    random.seed(time.time())

    if 'h' in args or 'help' in args:
        parser.print_help()
        sys.exit()
    
    bucket_name = 'testbucketx11'

    # Load metadata index and sets
    metadata = load_metadata_index(bucket_name)
    discarded = set(load_from_s3(bucket_name, config.DISCARDED_FILE) or [])
    notreleased = set(load_from_s3(bucket_name, config.NOTRELEASED_FILE) or [])

    # Log initial information
    Log(config.INFO, f'Metadata index loaded with {len(metadata)} entries')
    Log(config.INFO, f'{len(notreleased)} games not released yet')
    Log(config.INFO, f'{len(discarded)} apps discarded')

    try:
        Scraper(None, notreleased, discarded, args)
    except (KeyboardInterrupt, SystemExit):
        Log(config.INFO, 'Scraping interrupted. Progress saved.')
    finally:
        merge_chunks(bucket_name, config.UPDATE_OUTFILE)

    Log(config.INFO, 'Done')
