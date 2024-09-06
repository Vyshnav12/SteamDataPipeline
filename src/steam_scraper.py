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
from utils import load_from_s3, save_to_s3, ProgressLog, Log

def Scraper(dataset, notreleased, discarded, args, appIDs=None):
    '''
    Scrape Steam games.

    :param dataset: Dictionary with already scraped games.
    :param notreleased: List of games that are not yet released.
    :param discarded: List of games that are not games.
    :param args: Arguments from the command line.
    :param appIDs: List of games to scrape, if None, the list is requested to Steam.
    :return: None
    '''
    apps = []
    if appIDs is None:
        try:
            # Try to load applist from S3
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
                
                # Save the fetched list to S3
                save_to_s3(bucket_name, config.APPLIST_FILE, apps)
                Log(config.INFO, f'List with {len(apps)} games saved to S3.')
    else:
        apps = appIDs

    if apps:
        gamesAdded = 0
        gamesNotReleased = 0
        gamesdiscarded = 0
        successRequestCount = 0
        errorRequestCount = 0

        random.shuffle(apps)
        total = len(apps)
        count = 0

        start_time = dt.datetime.now()  # Record start time

        for appID in apps:
            if appID not in dataset and appID not in discarded:
                if args.released and appID in notreleased:
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

                        dataset[appID] = game
                        gamesAdded += 1

                        if appID in notreleased:
                            notreleased.remove(appID)

                        if args.autosave > 0 and gamesAdded % args.autosave == 0:
                            save_to_s3(bucket_name, config.DEFAULT_OUTFILE, dataset)
                    else:
                        if appID not in notreleased:
                            notreleased.append(appID)
                            gamesNotReleased += 1

                            if args.autosave > 0 and gamesNotReleased % args.autosave == 0:
                                save_to_s3(bucket_name, config.NOTRELEASED_FILE, notreleased)
                                
                else:
                    discarded.append(appID)
                    gamesdiscarded += 1

                if args.autosave > 0 and gamesdiscarded % args.autosave == 0:
                    save_to_s3(bucket_name, config.DISCARDED_FILE, discarded)

                time.sleep(args.sleep if random.random() > 0.1 else args.sleep * 2.0)
            count += 1
            ProgressLog('Scraping', count, total, start_time)

        ProgressLog('Scraping', total, total, start_time)
        print('\r')
        Log(config.INFO, f'Scrape completed: {gamesAdded} new games added, {gamesNotReleased} not released, {gamesdiscarded} discarded')
        save_to_s3(bucket_name, config.DEFAULT_OUTFILE, dataset)
        save_to_s3(bucket_name, config.DISCARDED_FILE, discarded)
        save_to_s3(bucket_name, config.NOTRELEASED_FILE, notreleased)
    else:
        Log(config.ERROR, 'Error requesting list of games')
        sys.exit()


def UpdateFromJSON(dataset, notreleased, discarded, args):
    '''
    Update using APPIDs from a JSON file. The JSON file must be in the format:
    {"applist": {"apps": [{"appid": "12345"}, {"appid": "67890"}, ...]}}
    '''
    bucket_name = 'steamscraperbucket'
    applist_key = config.APPLIST_FILE

    try:
        Log(config.INFO, f"Loading '{applist_key}' from S3")
        data = load_from_s3(bucket_name, applist_key)
        appIDs = [str(app["appid"]) for app in data["applist"]["apps"]]
        
        Log(config.INFO, f"Loaded {len(appIDs)} appIDs from '{applist_key}'")

        # Filter out appIDs already present in dataset or discarded or notreleased
        appIDs_to_update = [appID for appID in appIDs if appID not in dataset and appID not in discarded and appID not in notreleased]

        if len(appIDs_to_update) > 0:
            Log(config.INFO, f"New {len(appIDs_to_update)} appIDs to update")
            Scraper(dataset, notreleased, discarded, args, appIDs_to_update)
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
  parser.add_argument('-p', '--steamspy', type=str,   default=True,             help='Add SteamSpy info')
  parser.add_argument('-u', '--update',   type=str,   default='',               help='Update using APPIDs from a JSON file')
  args = parser.parse_args()
  random.seed(time.time())

  if 'h' in args or 'help' in args:
    parser.print_help()
    sys.exit()

  bucket_name = 'steamscraperbucket'
  dataset = load_from_s3(bucket_name, config.DEFAULT_OUTFILE)
  discarded = load_from_s3(bucket_name, config.DISCARDED_FILE)
  notreleased = load_from_s3(bucket_name, config.NOTRELEASED_FILE)

  if dataset is None:
    dataset = {}

  if discarded is None:
    discarded = []

  if notreleased is None:
    notreleased = []

  Log(config.INFO, f'Dataset loaded with {len(dataset)} games' if len(dataset) > 0 else 'New dataset created')

  if len(notreleased) > 0:
    Log(config.INFO, f'{len(notreleased)} games not released yet')

  if len(discarded) > 0:
    Log(config.INFO, f'{len(discarded)} apps discarded')

  try:
    if args.update == '':
      Scraper(dataset, notreleased, discarded, args)
    else:
      UpdateFromJSON(dataset, notreleased, discarded, args)
  except (KeyboardInterrupt, SystemExit):
    save_to_s3(bucket_name, config.DEFAULT_OUTFILE, dataset)
    save_to_s3(bucket_name, config.DISCARDED_FILE, discarded)
    save_to_s3(bucket_name, config.NOTRELEASED_FILE, notreleased)

  Log(config.INFO, 'Done')
