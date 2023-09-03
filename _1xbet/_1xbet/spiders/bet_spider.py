import json
from urllib.parse import urljoin

from scrapy import Spider, Request
from scrapy.utils.response import open_in_browser
from datetime import datetime


class BetSpiderSpider(Spider):
    name = "bet_spider"
    custom_settings = {
        "ZYTE_SMARTPROXY_ENABLED": True,
        'ZYTE_SMARTPROXY_APIKEY': '',   #TODO Enter your API Key
        'ZYTE_SMARTPROXY_PRESERVE_DELAY': True,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_zyte_smartproxy.ZyteSmartProxyMiddleware': 610
        },
        'FEED_URI': "2022_2023_data.xlsx",
        'FEED_FORMAT': 'xlsx',
        'FEED_EXPORT_ENCODING': 'utf-8-sig',
        'FEED_EXPORTERS': {'xlsx': 'scrapy_xlsx.XlsxItemExporter'},
    }
    headers = {
        'authority': '1xbet.whoscored.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'max-age=0',
        'sec-ch-ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
    }
    players_headers = {
        'authority': '1xbet.whoscored.com',
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'en-US,en;q=0.9',
        'model-last-mode': '',
        'referer': 'https://1xbet.whoscored.com/Matches/1640993/LiveStatistics/England-Premier-League-2022-2023-Newcastle-Leicester',
        'sec-ch-ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest'
    }
    counter = 0

    def start_requests(self):
        yield Request(url="https://1xbet.whoscored.com/", callback=self.parse, headers=self.headers)

    def parse(self, response, **kwargs):
        urls = response.css('a[href*="Premier-League"]::attr(href)').extract()
        for url in urls[:1]:
            url = urljoin("https://1xbet.whoscored.com/", url)
            yield Request(url=url, callback=self.parse_premier_league, headers=self.headers)

    def parse_premier_league(self, response):
        urls = []
        for item in response.css("#seasons option"):
            if item.css("::text").get('') in ['2022/2023']:
                temp = {
                    "season": item.css("::text").get(''),
                    "url": item.css("::attr(value)").get()
                }
                urls.append(temp)
        for url in urls:
            year_url = urljoin("https://1xbet.whoscored.com/", url['url'])
            year_20_21 = ['2022W31', '2022W32', '2022W33', '2022W34', '2022W35', '2022W36', '2022W37', '2022W38', '2022W39', '2022W40', '2022W41', '2022W42', '2022W43', '2022W44', '2022W45', '2022W46', '2022W47', '2022W48', '2022W49', '2022W50', '2022W51', '2022W52', '2023W01', '2023W02', '2023W03', '2023W04', '2023W05', '2023W06', '2023W07', '2023W08', '2023W09', '2023W10', '2023W11', '2023W12', '2023W13', '2023W14', '2023W15', '2023W16', '2023W17', '2023W18', '2023W19', '2023W20', '2023W21']
            yield Request(url=year_url, callback=self.parse_years, headers=self.headers, meta={"url": url,
                                                                                               "year_20_21": year_20_21,
                                                                                               "fresh_id": '',
                                                                                               "year": ''})

    def parse_years(self, response):
        fresh_id = response.meta['fresh_id']
        season = response.meta['url']['season']
        year_20_21 = response.meta['year_20_21']
        year = response.meta['year']
        url = response.meta['url']
        raw_data = \
            response.css('script:contains("tables.push")::text').get('').split("matches: ")[-1].split('isOptaStage')[
                0].replace("],"
                           "", "]").replace(",,", ',').replace(",]", "]").strip()
        if raw_data:
            data = eval(raw_data)
        else:
            data = eval(response.text)
        match_details = []
        for match in data:
            temp = {"match_id": match[0],
                    "match_date": match[2],
                    "match_time": match[3],
                    "match_team_1": match[5],
                    "match_team_2": match[8]}
            match_details.append(temp)
        if year:
            for detail in match_details:
                season = season.replace("/", "-")
                item = dict()
                item['season'] = season
                item['match_date'] = detail['match_date']
                item['match_time'] = detail['match_time']
                detail_url = f"https://1xbet.whoscored.com/Matches/{detail['match_id']}/live/England-Premier-League-{season}-{detail['match_team_1'].replace(' ', '-')}-{detail['match_team_2'].replace(' ', '-')}"
                yield Request(url=detail_url, callback=self.parse_match_details, headers=self.headers,
                              meta={"details": detail, "item": item, "year": year})
        tables_id = response.css('[id*="tournament-tables"]::attr(id)').get('').split('-')[-1]
        if tables_id:
            fresh_id = tables_id
        for year in year_20_21:
            year_20_21.remove(year)
            yield Request(url=f"https://1xbet.whoscored.com/tournamentsfeed/{fresh_id}/Fixtures/?d={year}&isAggregate=false",
                          callback=self.parse_years, meta={"url": url, "year_20_21": year_20_21, "fresh_id": fresh_id,
                                                           "year": year})


    def parse_match_details(self, response):
        year = response.meta['year']
        item = response.meta['item']
        raw_data = response.css('script:contains("matchCentreData")::text').get().replace(
            'require.config.params["args"] = ', '').strip().replace(";", '')
        raw_data = raw_data.split("matchCentreData: ")[-1].split("matchCentreEventTypeJson:")[0].strip()[:-1]
        data = json.loads(raw_data)
        home_team = data['home']
        away_team = data['away']
        details = response.meta['details']
        home_team_name = home_team['name']
        details['home_formation'] = "-".join(list(home_team['formations'][0]['formationName']))
        details['stadium'] = data['venueName']
        details['attendance'] = data['attendance']
        details['referee'] = data['referee']['name']
        home_team_id = home_team['teamId']
        details['match_date'] = data['timeStamp']
        item['match_date'] = data['timeStamp']
        away_team_name = away_team['name']
        away_team_id = away_team['teamId']
        season = item['season']
        details['away_formation'] = "-".join(list(away_team['formations'][0]['formationName']))
        temp = [{"home_team": home_team_name,
                 "home_id": home_team_id,
                 "away_team": away_team_name,
                 "away_id": away_team_id
                 },
                {"home_team": away_team_name,
                 "home_id": away_team_id,
                    "away_team": home_team_name,
                    "away_id": home_team_id}]
        for team in temp:
            model_value = \
                response.css('script:contains("gSiteHeaderValue")::text').get().strip().split("gSiteHeaderValue: ")[
                    -1].split(",")[0]
            details['home_team'] = team['home_team']
            details['away_team'] = team['away_team']
            team_id = team['home_id']
            url = f"https://1xbet.whoscored.com/StatisticsFeed/1/GetMatchCentrePlayerStatistics?category=summary&subcategory=all&statsAccumulationType=0&isCurrent=true&playerId=&teamIds={team_id}&matchId={details['match_id']}&stageId=&tournamentOptions=&sortBy=&sortAscending=&age=&ageComparisonType=&appearances=&appearancesComparisonType=&field=&nationality=&positionOptions=&timeOfTheGameEnd=&timeOfTheGameStart=&isMinApp=&page=&includeZeroValues=&numberOfPlayersToPick="
            self.players_headers['model-last-mode'] = model_value
            yield Request(url=url, callback=self.parse_player_details, headers=self.players_headers,
                          meta={"details": details, "team": team, "item": item, "year": year})

    def parse_player_details(self, response):
        item = response.meta['item']
        details = response.meta['details']
        team = response.meta['team']
        data = response.json()
        players = data['playerTableStats']
        year = response.meta['year']
        for raw_player in players:
            self.counter += 1
            players_details = dict()
            players_details['ID'] = self.counter
            players_details['player'] = raw_player['name']
            players_details['Match Week'] = year.split("W")[-1]
            match_date = item['match_date']
            datetime_object = datetime.strptime(match_date, '%Y-%m-%d %H:%M:%S')
            day_name = datetime_object.strftime('%A')
            players_details['match_date'] = item['match_date']
            players_details['season'] = item['season']
            players_details['match_day'] = day_name
            players_details['players_team'] = team['home_team']
            players_details['opponent_team'] = team['away_team']
            players_details['position'] = raw_player['playedPositionsShort']
            players_details['match_time'] = item['match_time']
            players_details['stadium'] = details['stadium']
            players_details['players_team_formation'] = details['home_formation']
            players_details['opponent_team_formation'] = details['away_formation']
            players_details['referee'] = details['referee']
            players_details['match_captain'] = ''
            players_details['is_player_captain'] = ''
            players_details['attendance'] = details['attendance']
            players_details['total'] = raw_player['shotsTotal']
            players_details['shotot'] = raw_player['shotOnTarget']
            players_details['KeyPassTotal'] = raw_player['keyPassTotal']
            players_details['PassSuccessInMatch'] = raw_player['passSuccessInMatch']
            players_details['duelAerialWon'] = raw_player['duelAerialWon']
            players_details['Touches'] = raw_player['touches']
            players_details['Last_Player_rating'] = ''
            players_details['Avg_player_rating'] = ''
            players_details['Squad_Last_rating'] = ''
            players_details['Squad_Avg_att_rating'] = ''
            players_details['Squad_Avg_mid_rating'] = ''
            players_details['Squad_Avg_def_rating'] = ''
            players_details['Squad_Avg_gk_rating'] = ''
            players_details['Squad_Avg_GF'] = ''
            players_details['Squad_avg_GA'] = ''
            players_details['Opp_Avg_team_rating'] = ''
            players_details['Opp_Last_Team_rating'] = ''
            players_details['Opp_avg_att_rating'] = ''
            players_details['Opp_avg_mid_rating'] = ''
            players_details['Opp_avg_def_rating'] = ''
            players_details['Opp_avg_gk_rating'] = ''
            players_details['Opp_Avg_team_GF'] = ''
            players_details['Opp_Avg_team_GA'] = ''
            players_details['Player_Rating'] = raw_player['rating']
            yield players_details

