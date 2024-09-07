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

# Modifications:
# - Replaced saveJSON/loadJSON with function to load and save to Amazon S3.
# - Modified `ParseSteamGame` function to be more readable, removed certain fields from being processed, and ensure missing fields are processed safely.
# - Improved performance and error handling in the `Scraper` function.
# - Added functionality to check Steam IDs against `applist.json` to avoid redundant API calls.
# - Enhanced logging for better tracking of scraping progress and errors.
# - Split functions into different files and organized imports.
#
# Modified by Vyshnav Varma <vyshnavvarma@gmail.com>
# Date: 01/09/2024
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
from utils import load_from_s3, save_to_s3, ProgressLog, Log, save_chunk_to_s3, merge_chunks, load_metadata_index, save_metadata_index, is_appID_present, update_metadata_index, list_chunk_filenames

def Scraper(dataset, notreleased, discarded, args, appIDs=None):
    '''
    Scrapes Steam and SteamSpy for game data and saves it to S3.

    Args:
        dataset (str): The name of the dataset to scrape.
        notreleased (set): A set of appIDs that are not released.
        discarded (set): A set of appIDs that are discarded.
        args (argparse.Namespace): The command line arguments.
        appIDs (list, optional): A list of appIDs to scrape. Defaults to None.
    '''
    
    bucket_name = 'steamscraperbucket'
    metadata = load_metadata_index(bucket_name)  # Load existing metadata index as a set

    apps = []
    if appIDs is None:
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
    else:
        apps = appIDs

    # Convert notreleased and discarded to sets for faster lookups
    notreleased_set = set(notreleased)
    discarded_set = set(discarded)

    if apps:
        gamesAdded = 0
        gamesNotReleased = 0
        gamesdiscarded = 0
        successRequestCount = 0
        errorRequestCount = 0

        random.shuffle(apps)
        total = len(apps) - len(discarded_set) - len(notreleased_set)
        count = 0

        start_time = dt.datetime.now()
        chunk_size = 10000
        chunk = {}
        manifest = load_from_s3(bucket_name, 'manifest.json') or {'chunks': []}

        for appID in apps:
            if appID not in metadata and appID not in discarded_set:
                if args.released and appID in notreleased_set:
                    continue

                app = SteamRequest(appID, min(4, args.sleep), successRequestCount, errorRequestCount, args.retries)
                if app:
                    game = ParseSteamGame(app)
                    if game['release_date'] != '':
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
                                    'user_score': 0,
                                    'score_rank': "",
                                    'positive': 0,
                                    'negative': 0,
                                    'estimated_owners': "0 - 0",
                                    'average_playtime_forever': 0,
                                    'average_playtime_2weeks': 0,
                                    'median_playtime_forever': 0,
                                    'median_playtime_2weeks': 0,
                                    'peak_ccu': 0,
                                    'tags': []
                                })

                        chunk[appID] = game
                        gamesAdded += 1
                        count += 1
                        ProgressLog('Scraping', count, total, start_time)

                        if appID in notreleased_set:
                            notreleased_set.remove(appID)

                        if len(chunk) >= chunk_size:
                            manifest = save_chunk_to_s3(bucket_name, chunk, manifest)
                            metadata = update_metadata_index(metadata, chunk)
                            # Update metadata index
                            Log(config.INFO, f'Updated metadata index with chunk. Current metadata size: {len(metadata)}')  # Log metadata size
                            chunk.clear()
                    else:
                        if appID not in notreleased_set:
                            notreleased_set.add(appID)
                            gamesNotReleased += 1

                            if args.autosave > 0 and gamesNotReleased % args.autosave == 0:
                                save_to_s3(bucket_name, config.NOTRELEASED_FILE, list(notreleased_set))
                else:
                    discarded_set.add(appID)
                    gamesdiscarded += 1

                if args.autosave > 0 and gamesdiscarded % args.autosave == 0:
                    save_to_s3(bucket_name, config.DISCARDED_FILE, list(discarded_set))

                time.sleep(args.sleep if random.random() > 0.1 else args.sleep * 2.0)

        # Save any remaining data
        if chunk:
            manifest = save_chunk_to_s3(bucket_name, chunk, manifest)
            metadata = update_metadata_index(metadata, chunk)  # Update metadata index

        ProgressLog('Scraping', total, total, start_time)
        print('\r')
        Log(config.INFO, f'Scrape completed: {gamesAdded} new games added, {gamesNotReleased} not released, {gamesdiscarded} discarded')
        save_to_s3(bucket_name, config.DISCARDED_FILE, list(discarded_set))
        save_to_s3(bucket_name, config.NOTRELEASED_FILE, list(notreleased_set))

        # Finalize metadata and chunk merge
        save_metadata_index(bucket_name, metadata)
        merge_chunks(bucket_name, config.DEFAULT_OUTFILE)
    else:
        Log(config.ERROR, 'Error requesting list of games')
        sys.exit()


def UpdateFromJSON(dataset, notreleased, discarded, args):

    """
    Update the metadata index from a JSON file.
    Args:
        dataset (str): The name of the dataset to update.
        notreleased (set): A set of appIDs that are not released.
        discarded (set): A set of appIDs that are discarded.
        args (argparse.Namespace): The command line arguments.

    Returns:
        None
    """
    bucket_name = 'steamscraperbucket'
    applist_key = config.APPLIST_FILE

    try:
        Log(config.INFO, f"Loading '{applist_key}' from S3")
        data = load_from_s3(bucket_name, applist_key)
        appIDs = [str(app["appid"]) for app in data["applist"]["apps"]]
        
        Log(config.INFO, f"Loaded {len(appIDs)} appIDs from '{applist_key}'")

        # Load metadata index
        metadata = load_metadata_index(bucket_name)

        # Convert notreleased and discarded to sets for faster lookups
        notreleased_set = set(notreleased)
        discarded_set = set(discarded)

        # Filter out appIDs already present in metadata, discarded, or notreleased
        appIDs_to_update = [appID for appID in appIDs if not is_appID_present(metadata, appID) and appID not in discarded_set and appID not in notreleased_set]

        if len(appIDs_to_update) > 0:
            Log(config.INFO, f"New {len(appIDs_to_update)} appIDs to update")
            Scraper(dataset, list(notreleased_set), list(discarded_set), args, appIDs_to_update)
        else:
            Log(config.WARNING, f'No new appIDs to update from {applist_key}')
    except Exception as e:
        Log(config.ERROR, f'Error loading or processing file from S3: {str(e)}')


if __name__ == "__main__":
    Log(config.INFO, f'Steam Games Scraper {__version__} by {__author__}')
  
    parser = argparse.ArgumentParser(description='Steam games scraper.')
    parser.add_argument('-i', '--infile',   type=str,   default=config.DEFAULT_OUTFILE,  help='Input file name')
    parser.add_argument('-o', '--outfile',  type=str,   default=config.DEFAULT_OUTFILE,  help='Output file name')
    parser.add_argument('-s', '--sleep',    type=float, default=config.DEFAULT_SLEEP,    help='Waiting time between requests')
    parser.add_argument('-r', '--retries',  type=int,   default=config.DEFAULT_RETRIES,  help='Number of retries (0 to always retry)')
    parser.add_argument('-a', '--autosave', type=int,   default=config.DEFAULT_AUTOSAVE, help='Record the data every number of new entries (0 to deactivate)')
    parser.add_argument('-d', '--released', type=bool,  default=True,             help='If it is on the list of not yet released, no information is requested')
    parser.add_argument('-c', '--currency', type=str,   default=config.DEFAULT_CURRENCY, help='Currency code')
    parser.add_argument('-l', '--language', type=str,   default=config.DEFAULT_LANGUAGE, help='Language code')
    parser.add_argument('-p', '--steamspy', type=bool,  default=True,             help='Add SteamSpy info')
    parser.add_argument('-u', '--update',   type=str,   default='',               help='Update using APPIDs from a JSON file')
    args = parser.parse_args()
    random.seed(time.time())

    if 'h' in args or 'help' in args:
        parser.print_help()
        sys.exit()
    
    bucket_name = 'steamscraperbucket'

    # Load metadata index and chunked data
    metadata = load_metadata_index(bucket_name)
    discarded = load_from_s3(bucket_name, config.DISCARDED_FILE) or []
    notreleased = load_from_s3(bucket_name, config.NOTRELEASED_FILE) or []

    # Convert lists to sets for faster lookups
    discarded_set = set(discarded)
    notreleased_set = set(notreleased)

    # Initialize dataset
    dataset = {}
    for chunk_file in list_chunk_filenames(bucket_name):
        chunk = load_from_s3(bucket_name, chunk_file) or {}
        dataset.update(chunk)

    Log(config.INFO, f'Dataset loaded with {len(dataset)} games' if len(dataset) > 0 else 'New dataset created')

    if len(notreleased_set) > 0:
        Log(config.INFO, f'{len(notreleased_set)} games not released yet')

    if len(discarded_set) > 0:
        Log(config.INFO, f'{len(discarded_set)} apps discarded')

    try:
        if args.update == '':
            Scraper(dataset, list(notreleased_set), list(discarded_set), args)
        else:
            UpdateFromJSON(dataset, list(notreleased_set), list(discarded_set), args)
    except (KeyboardInterrupt, SystemExit):
        save_to_s3(bucket_name, config.DISCARDED_FILE, list(discarded_set))
        save_to_s3(bucket_name, config.NOTRELEASED_FILE, list(notreleased_set))

    Log(config.INFO, 'Done')
