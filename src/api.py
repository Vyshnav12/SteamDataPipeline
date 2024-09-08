import sys
import re
import requests
from utils import SanitizeText, Log, PriceToFloat
from ssl import SSLError
import time
import traceback
import config
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException


def DoRequest(url, parameters=None, retryTime=5, successCount=0, errorCount=0, retries=0, headers=None):
    '''
    Makes a Web request. If an error occurs, retry.
    '''
    response = None
    try:
        # Make request with custom headers
        response = requests.get(url=url, params=parameters, timeout=config.DEFAULT_TIMEOUT, allow_redirects=True, headers=headers)
        response.raise_for_status()  # Raise HTTPError for bad responses
    except (HTTPError, ConnectionError, Timeout, RequestException, SSLError) as ex:
        Log(config.EXCEPTION, f'An exception of type {type(ex).__name__} occurred: {ex}')
        response = None

    if response and response.status_code == 200:
        errorCount = 0
        successCount += 1
        if successCount > retryTime:
            retryTime = min(5, retryTime / 2)
            successCount = 0
    else:
        if retries == 0 or errorCount < retries:
            errorCount += 1
            successCount = 0
            retryTime = min(retryTime * 2, 500)
            if response is not None:
                Log(config.WARNING, f'{response.reason}, retrying in {retryTime} seconds')
            else:
                Log(config.WARNING, f'Request failed, retrying in {retryTime} seconds.')

            time.sleep(retryTime)
            return DoRequest(url, parameters, retryTime, successCount, errorCount, retries, headers)
        else:
            print('[!] No more retries.')
            sys.exit()

    return response

def SteamRequest(appID, retryTime, successRequestCount, errorRequestCount, retries, currency=config.DEFAULT_CURRENCY, language=config.DEFAULT_LANGUAGE):
  '''
  Request and parse information about a Steam app.
  '''
  url = "http://store.steampowered.com/api/appdetails/"
  response = DoRequest(url, {"appids": appID, "cc": currency, "l": language}, retryTime, successRequestCount, errorRequestCount, retries)
  if response:
    try:
      data = response.json()
      app = data[appID]
      if app['success'] is False:
        return None
      elif app['data']['type'] != 'game':
        return None
      elif app['data']['is_free'] is False and 'price_overview' in app['data'] and app['data']['price_overview']['final_formatted'] == '':
        return None
      elif 'developers' in app['data'] and len(app['data']['developers']) == 0:
        return None
      else:
        return app['data']
    except Exception as ex:
      Log(config.EXCEPTION, f'An exception of type {ex} ocurred. Traceback: {traceback.format_exc()}')
      return None
  else:
    Log(config.ERROR, 'Bad response')
    return None

def SteamSpyRequest(appID, retryTime, successRequestCount, errorRequestCount, retries):
    '''
    Request and parse information about a Steam app using SteamSpy, handling rate limiting and connection errors.
    '''
    url = f"https://steamspy.com/api.php?request=appdetails&appid={appID}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36'
    }
    
    response = DoRequest(url, None, retryTime, successRequestCount, errorRequestCount, retries, headers)
    if response:
        try:
            response_text = response.text.strip()

            # Log the raw response for debugging
            Log(config.INFO, f'Response from SteamSpy API for appID {appID}: {response_text[:200]}...')

            # Handle SteamSpy rate limiting
            if "Too many connections" in response_text:
                Log(config.WARNING, f"Rate limit exceeded for appID {appID}. Retrying after a delay.")
                time.sleep(10) 
                return SteamSpyRequest(appID, retryTime, successRequestCount, errorRequestCount, retries)

            # Check if the response is valid JSON
            if response_text:
                data = response.json()  # Try to parse the JSON response
                if data.get('developer', "") != "":
                    return data
                else:
                    return None
            else:
                Log(config.WARNING, f"Empty response for appID {appID}")
                return None

        except Exception as ex:
            Log(config.EXCEPTION, f'An exception of type {ex} occurred while parsing JSON. Traceback: {traceback.format_exc()}')
            return None
    else:
        Log(config.ERROR, 'Bad response from SteamSpy API')
        return None


def ParseSteamGame(app):
  '''
  Parse game info.
  '''
  game = {}
  # Basic Info
  game['name'] = app.get('name', '').strip()
  game['release_date'] = app.get('release_date', {}).get('date', '') if 'release_date' in app and not app['release_date'].get('coming_soon', False) else ''
  game['required_age'] = int(str(app.get('required_age', 0)).replace('+', ''))

  # Pricing and DLC
  game['price'] = 0.0 if app.get('is_free') or 'price_overview' not in app else PriceToFloat(app['price_overview'].get('final_formatted', ''))
  game['dlc_count'] = len(app.get('dlc', []))

  # Descriptions
  game['detailed_description'] = app.get('detailed_description', '').strip()
  game['about_the_game'] = app.get('about_the_game', '').strip()
  game['short_description'] = app.get('short_description', '').strip()

  # Technical Details
  game['windows'] = app.get('platforms', {}).get('windows', False)
  game['mac'] = app.get('platforms', {}).get('mac', False)
  game['linux'] = app.get('platforms', {}).get('linux', False)
  game['metacritic_score'] = int(app.get('metacritic', {}).get('score', 0))
  game['achievements'] = int(app.get('achievements', {}).get('total', 0))
  game['recommendations'] = app.get('recommendations', {}).get('total', 0)
  game['notes'] = app.get('content_descriptors', {}).get('notes', '')

  # Languages
  game['supported_languages'] = []
  game['full_audio_languages'] = []
  if 'supported_languages' in app:
    languagesApp = re.sub('<[^<]+?>', '', app['supported_languages'])
    languagesApp = languagesApp.replace('languages with full audio support', '')
    languages = languagesApp.split(', ')
    for lang in languages:
      if '*' in lang:
        game['full_audio_languages'].append(lang.replace('*', ''))
      game['supported_languages'].append(lang.replace('*', ''))

  # Packages
  game['packages'] = []
  if 'package_groups' in app:
    for package in app['package_groups']:
      subs = []
      if 'subs' in package:
        for sub in package['subs']:
          subs.append({'text': SanitizeText(sub['option_text']),
                       'description': sub.get('option_description', ''),
                       'price': round(float(sub.get('price_in_cents_with_discount', 0)) * 0.01, 2) })
      game['packages'].append({'title': SanitizeText(package.get('title', '')), 
                               'description': SanitizeText(package.get('description', '')), 
                               'subs': subs})

  # Developers, Publishers, Categories, Genres
  game['developers'] = [developer.strip() for developer in app.get('developers', [])]
  game['publishers'] = [publisher.strip() for publisher in app.get('publishers', [])]
  game['categories'] = [category.get('description', '') for category in app.get('categories', [])]
  game['genres'] = [genre.get('description', '') for genre in app.get('genres', [])]

  game['detailed_description'] = SanitizeText(game['detailed_description'])
  game['about_the_game'] = SanitizeText(game['about_the_game'])
  game['short_description'] = SanitizeText(game['short_description'])
  game['notes'] = SanitizeText(game['notes'])

  return game