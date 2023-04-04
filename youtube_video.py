from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import HttpRequest
import requests
import pandas as pd
import json
from utils import duration_to_second, utc_to_local, get_current_time, covert_to_millions

class YoutubeVideo:
    YOUTUBE_API_SERVICE_NAME = "youtube"
    YOUTUBE_API_VERSION = "v3"
    ID_SIZE_LIMIT = 50
    YOUTUBE_SEARCH_API_URL = "https://youtube-search-and-download.p.rapidapi.com/trending"

    def __init__(self, youtube_api_key: str, rapid_api_key: str):
        self.youtube_api_key = youtube_api_key
        self.rapid_api_key = rapid_api_key
        try:
            self.youtube = build(self.YOUTUBE_API_SERVICE_NAME, self.YOUTUBE_API_VERSION, developerKey=self.youtube_api_key)
        except Exception as e:
            print(f"Found error when building youtube video: {e}")
            raise


    def get_category(self, category_id: str) -> str:
        """ Return category title of a given category id """
        
        category_data = self.youtube.videoCategories().list(part='snippet', id=category_id).execute()

        try:
            # Extract the category name from the category data
            category_name = category_data['items'][0]['snippet']['title']
        except:
            print("Something went wrong in get_category_name, please check the API docs")
            return ""
        
        return category_name
    

    def get_trending_ids(self) -> list:
        """
        Return a list of ids of trending "now" videos

        Use the Youtube Search and Download API because it returns videos 
        the same order as in https://www.youtube.com/feed/trending
        API can be found here: https://rapidapi.com/h0p3rwe/api/youtube-search-and-download
        
        Type of trending videos:
        n - now (default)
        mu - music
        mo - movies
        g - gaming
        """

        querystring = {"type":"n","hl":"en","gl":"US"}
        url = self.YOUTUBE_SEARCH_API_URL
        headers = {
            "X-RapidAPI-Key": self.rapid_api_key,
            "X-RapidAPI-Host": "youtube-search-and-download.p.rapidapi.com"
        }

        try:
            response = requests.request("GET", url, headers=headers, params=querystring)
            response_json = response.json()  
            contents = response_json['contents']
            ids = [content["video"]["videoId"] for content in contents]

            return ids
        
        except Exception as e:
            if "message" in response_json:
                e = response_json["message"]
            print(f"Rapid API error: {e}")
            raise
        

    def handle_api_error(self, e: HttpError):   
        try:
            error_content = json.loads(e.content)
            error_msg = error_content.get('error', {}).get('message')
        except:
            error_msg = e

        print(f"API error: {error_msg}")
        raise


    def get_response_items(self, request: HttpRequest):
        try:
            response = request.execute()
            items = response['items']
            if len(items) == 0:
                print(f"No response items returned from the API")
                return None

            return items
        
        except HttpError as e:
            self.handle_api_error(e)
            return None


    def get_videos_by_ids(self, ids: list) -> pd.DataFrame:
        """
        Return a dataframe of Youtube videos
        Refer to https://developers.google.com/youtube/v3/docs/videos/list
        """
        # will get 400 error code when there are too many ids in `id` input
        if  len(ids) > self.ID_SIZE_LIMIT:
            print(f"Please make sure the # of id <= {self.ID_SIZE_LIMIT}.")
            return []
        
        # store attributes of videos
        video_ids = []
        titles = []
        all_published_at = []
        durations = []
        views = []
        categories = []
        all_tags = []
        channel_ids = []
        channel_titles = []
        all_extracted_at = []
        ranks = []
        rank = 1
        df = pd.DataFrame()

        # call the API to get videos by ids
        request = self.youtube.videos().list(
            part="snippet,contentDetails,statistics",
            maxResults=self.ID_SIZE_LIMIT,
            id=ids
        )

        items = self.get_response_items(request)
        for item in items:
            try:
                try:
                    tags = item["snippet"]["tags"]
                except:
                    tags = ""
                
                try:
                    published_at = utc_to_local(item["snippet"]["publishedAt"])
                except:
                    published_at = ""
                    
                video_ids.append(item["id"])
                titles.append(item["snippet"]["title"])
                durations.append(duration_to_second(item["contentDetails"]["duration"]))
                views.append(covert_to_millions(item["statistics"].get("viewCount", 0)))
                categories.append(self.get_category(item["snippet"]["categoryId"]))
                channel_ids.append(item["snippet"]["channelId"])
                channel_titles.append(item["snippet"]["channelTitle"])
                all_tags.append(tags)
                all_published_at.append(published_at)
                all_extracted_at.append(get_current_time())
                ranks.append(rank)
                rank += 1
            except Exception as e:
                print(f"Something went wrong in get_videos_by_ids: {e}")
                return df

        df = pd.DataFrame({
            'video_id': video_ids,
            'title': titles,
            'duration_sec': durations,
            'views_millions': views,
            'category': categories,
            'channel_id': channel_ids,
            'channel_title': channel_titles,
            'tags': all_tags,
            'published_at': all_published_at,
            'extracted_at': all_extracted_at,
            'rank': ranks
        })

        return df

    
    def get_channels_by_ids(self, ids: list) -> pd.DataFrame:
        """
        Return a dataframe of Youtube channels
        Refer to https://developers.google.com/youtube/v3/docs/channels/list
        """
        
        # will get 400 error code when there are too many video ids in `id` input
        if  len(ids) > self.ID_SIZE_LIMIT:
            print(f"Please make sure the # of id <= {self.ID_SIZE_LIMIT}.")
            return []

        # store attributes of channels
        channel_ids = []
        titles = []
        urls = []
        countries = []
        all_published_at = []

        # call the API to get channels by ids
        request = self.youtube.channels().list(
            part="snippet",
            maxResults=self.ID_SIZE_LIMIT,
            id=ids
        )
        items = self.get_response_items(request)

        for item in items:
            try:
                try:
                    url = item["snippet"]["customUrl"]
                except:
                    url = ""
                try:
                    country = item["snippet"]["country"]
                except:
                    country = ""
                try:
                    published_at = utc_to_local(item["snippet"]["publishedAt"])
                except:
                    published_at = ""

                channel_ids.append(item["id"])
                titles.append(item["snippet"]["title"])
                urls.append(url)
                countries.append(country)
                all_published_at.append(published_at)
            except Exception as e:
                print(f"Something went wrong in get_channels_by_ids: {e}")
                return []
            
        # save info of channels as dataframe
        df = pd.DataFrame({
            'channel_id': channel_ids,
            'channel_title': titles,
            'custom_url': urls,
            'country': countries,
            'published_at': all_published_at
        })

        return df
    

    def get_combined_data(self, ids: list, request_method: callable, chunk_size: int) -> pd.DataFrame:
        """ Concat the result of calling request_method
            whose parameter is the ids split by chunk_size
        """

        chunk_index = 0
        df_combined = pd.DataFrame()

        while ids:
            try:
                chunk_ids, ids = ids[:chunk_size], ids[chunk_size:]
                chunk_data = request_method(chunk_ids)
                if "rank" in chunk_data.columns:
                    # store the index of ranking
                    chunk_data["rank"] = chunk_data["rank"] + chunk_index * 50
                    chunk_index += 1
                df_combined = pd.concat([df_combined, chunk_data])

            except Exception as e:
                print(f"Found error in get_combined_data: {e}")
                raise

        return df_combined
    
    
    def get_all_videos(self, ids: list) -> pd.DataFrame:
        all_videos = self.get_combined_data(ids=ids, 
                                            request_method=self.get_videos_by_ids, 
                                            chunk_size=self.ID_SIZE_LIMIT)

        if "video_id" in all_videos.columns:
            # drop duplicates
            all_videos = all_videos.drop_duplicates(subset=["video_id"])

        return all_videos
    
    def get_all_channels(self, ids: list) -> pd.DataFrame:
        all_channels = self.get_combined_data(ids=ids, 
                                              request_method=self.get_channels_by_ids, 
                                              chunk_size=self.ID_SIZE_LIMIT)
        if "channel_id" in all_channels.columns:
            # drop duplicates
            all_channels = all_channels.drop_duplicates(subset=["channel_id"])

        return all_channels
