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
  url = "https://store.steampowered.com/api/appdetails/"  # Use HTTPS
  params = {"appids": appID, "cc": currency, "l": language}
  response = DoRequest(url, params, retryTime, successRequestCount, errorRequestCount, retries)
  
  if not response:
      Log(config.ERROR, 'Bad response')
      return None

  try:
      data = response.json()
      app = data.get(str(appID), {})
      
      if not app.get('success'):
          return None

      app_data = app.get('data', {})
      
      if (app_data.get('type') != 'game' or
          (not app_data.get('is_free') and 
           'price_overview' in app_data and 
           app_data['price_overview'].get('final_formatted') == '') or
          not app_data.get('developers')):
          return None

      return app_data
  except Exception as ex:
      Log(config.EXCEPTION, f'An exception occurred: {ex}. Traceback: {traceback.format_exc()}')
      return None

def SteamSpyRequest(appID, retryTime, successRequestCount, errorRequestCount, retries):
    '''
    Request and parse information about a Steam app using SteamSpy, handling rate limiting and connection errors.
    '''
    url = f"https://steamspy.com/api.php"
    params = {"request": "appdetails", "appid": appID}
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36'
    }
    
    response = DoRequest(url, params, retryTime, successRequestCount, errorRequestCount, retries, headers)
    if not response:
        Log(config.ERROR, 'Bad response from SteamSpy API')
        return None

    try:
        response_text = response.text.strip()

        if "Too many connections" in response_text:
            Log(config.WARNING, f"Rate limit exceeded for appID {appID}. Retrying after a delay.")
            return None

        if not response_text:
            Log(config.WARNING, f"Empty response for appID {appID}")
            return None

        data = response.json()
        return data if data.get('developer') else None

    except Exception as ex:
        Log(config.EXCEPTION, f'An exception occurred while parsing JSON for appID {appID}: {ex}')
        return None

def ParseSteamGame(app):
  '''
  Parse game info.
  '''
  game = {
      # Basic Info
      'name': app.get('name', '').strip(),
      'release_date': app.get('release_date', {}).get('date', '') if not app.get('release_date', {}).get('coming_soon', False) else '',
      'required_age': int(str(app.get('required_age', 0)).replace('+', '')),

      # Pricing and DLC
      'price': 0.0 if app.get('is_free') else PriceToFloat(app.get('price_overview', {}).get('final_formatted', '')),
      'dlc_count': len(app.get('dlc', [])),

      # Descriptions
      'detailed_description': app.get('detailed_description', '').strip(),
      'about_the_game': app.get('about_the_game', '').strip(),
      'short_description': app.get('short_description', '').strip(),

      # Technical Details
      'windows': app.get('platforms', {}).get('windows', False),
      'mac': app.get('platforms', {}).get('mac', False),
      'linux': app.get('platforms', {}).get('linux', False),
      'metacritic_score': int(app.get('metacritic', {}).get('score', 0)),
      'achievements': int(app.get('achievements', {}).get('total', 0)),
      'recommendations': app.get('recommendations', {}).get('total', 0),
      'notes': app.get('content_descriptors', {}).get('notes', ''),

      # Languages
      'supported_languages': [],
      'full_audio_languages': [],

      # Packages
      'packages': [],

      # Developers, Publishers, Categories, Genres
      'developers': [developer.strip() for developer in app.get('developers', [])],
      'publishers': [publisher.strip() for publisher in app.get('publishers', [])],
      'categories': [category.get('description', '') for category in app.get('categories', [])],
      'genres': [genre.get('description', '') for genre in app.get('genres', [])],
  }

  # Languages
  if 'supported_languages' in app:
      languagesApp = re.sub('<[^<]+?>', '', app['supported_languages'])
      languagesApp = languagesApp.replace('languages with full audio support', '')
      for lang in languagesApp.split(', '):
          clean_lang = lang.replace('*', '')
          game['supported_languages'].append(clean_lang)
          if '*' in lang:
              game['full_audio_languages'].append(clean_lang)

  # Packages
  for package in app.get('package_groups', []):
      subs = [
          {
              'text': SanitizeText(sub['option_text']),
              'description': sub.get('option_description', ''),
              'price': round(float(sub.get('price_in_cents_with_discount', 0)) * 0.01, 2)
          }
          for sub in package.get('subs', [])
      ]
      game['packages'].append({
          'title': SanitizeText(package.get('title', '')),
          'description': SanitizeText(package.get('description', '')),
          'subs': subs
      })

  # Sanitize descriptions
  for key in ['detailed_description', 'about_the_game', 'short_description', 'notes']:
      game[key] = SanitizeText(game[key])

  return game