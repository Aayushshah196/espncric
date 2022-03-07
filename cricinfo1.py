# -*- coding: utf-8 -*-
import scrapy, datetime, os, pandas
from scrapy.crawler import CrawlerProcess
import datetime
from bs4 import BeautifulSoup
import json
import requests
import xlrd
import pathlib
import os
from tqdm import tqdm
from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
import scrapy.utils.misc 
import scrapy.core.scraper
# from scrapy import signals
# from scrapy.xlib.pydispatch import dispatcher
import time

def warn_on_generator_with_return_value_stub(spider, callable):
    pass

scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub

# Problem in matchdata in matchparse2 in match-info-match
class CricinfoSpider(scrapy.Spider):
    name = 'cricinfo'
    file_name = 'MBData.csv'
    allowed_domains = ['espncricinfo.com']
    custom_settings = {'LOG_LEVEL': 'INFO',
                      'RETRY_ENABLED': True,
                      'FEED_FORMAT': 'csv',
                      'FEED_URI': file_name,
                      'LOG_FILE': file_name.replace('csv', 'log')
                      }
    unique_players = {}


    def __init__(self):
        print("-------------------------------------------------------------------------------------------")
        try:
            self.old_matches = pandas.read_csv('MBData.csv')['mid'].unique().tolist()
            self.old_matches = [str(x) for x in self.old_matches]
            print(len(self.old_matches))
        except:
            self.old_matches = []


    def start_requests(self):
        start_date = datetime.date(2017, 1, 7)
        end_date = datetime.datetime.today()
        season_list = []
        edy = end_date.year
        for i in range(1, 2 * (edy - start_date.year)):
            season_list.append(str(start_date.year - 1) + "/" + str(start_date.year)[-2:])
            season_list.append(str(start_date.year))
            start_date = start_date + datetime.timedelta(days=365)
            if start_date.year == edy:
                break
        
        # Append the end_year to the list
        if str(edy) not in season_list:
            season_list.append(str(edy))

        # Append series for previous and current year if not appended
        if str(end_date.year - 1) + "/" + str(end_date.year)[-2:] not in season_list:
            season_list.append( (str(end_date.year - 1) + "/" + str(end_date.year)[-2:]) )

        # Append series for current and next year if not appended
        if str(end_date.year) + "/" + str(end_date.year+1)[-2:] not in season_list:
            season_list.append( (str(end_date.year) + "/" + str(end_date.year+1)[-2:]) )

        print("-------------------------------------------------------------------------------------------")
        print(f"Total season list from {start_date} to {end_date.year} : {len(season_list)}")
        print("-------------------------------------------------------------------------------------------")

        for season in season_list:
            url = "https://www.espncricinfo.com/ci/engine/series/index.html?season={};view=season".format(
                season.replace('/', '%2F'))
            yield scrapy.Request(url=url,callback=self.parse,meta={'season':season})


    def parse(self, response):
        series_data = []
        d = {}
        yes = []

        wb = pandas.read_csv("Filters.csv")

        for i in range(7):
            cell_value_class = wb.iloc[i]["Filter"] #sheet.cell(i, 0).value
            cell_value_id = (wb.iloc[i]["Value"]) #int(sheet.cell(i, 1).value)
            d[cell_value_class] = True if cell_value_id == 1 else False
            if cell_value_id == 1:
                if "men" not in cell_value_class:
                    yes.append(cell_value_class.lower())


        soup = BeautifulSoup(response.body, 'lxml')
        match_section = soup.find_all('div', {'class': 'match-section-head'})
        count = 0

        for section in match_section:
            if d['Men&Women']:
                if (str(section.text).lower() in yes) and (count<=2):
                    frmt = str(section.text)
                    count += 1
                    section = section.find_next('section')
                    section_series = section.find_all('section', {'class': 'series-summary-block'})
                    for s in section_series:
                        if "Forthcoming series" not in str(s):
                            series_url = "https://www.espncricinfo.com" +s.get('data-summary-url')
                            # series_id = series_url.split('=')[-1]
                            series = s.find('div',{'class':'series-info'}).find('a').text
                            yield scrapy.Request(url=series_url,callback=self.parse_series,meta={'season':response.meta['season'],'series_id':s.get('data-series-id'),'series':series,'in':'1','format':frmt})

            elif d['Men Only']:

                if (str(section.text).lower() in yes) and (count<=2) and "women" not in str(section.text).lower():
                    frmt = str(section.text)
                    count += 1
                    section = section.find_next('section')
                    section_series = section.find_all('section', {'class': 'series-summary-block'})

                    for s in section_series:
                        if "Forthcoming series" not in str(s):
                            series_url = "https://www.espncricinfo.com" +s.get('data-summary-url')
                            # series_id = series_url.strip().split('=')[-1]
                            series = s.find('div',{'class':'series-info'}).find('a').text
                            yield scrapy.Request(url=series_url,callback=self.parse_series,meta={'season':response.meta['season'],'series_id':s.get('data-series-id'),'series':series,'in':'1','format':frmt})
            else:
                if (str(section.text).lower() in yes) and (count<=2) and "women" in str(section.text).lower():
                    count += 1
                    frmt = str(section.text)
                    section = section.find_next('section')
                    section_series = section.find_all('section', {'class': 'series-summary-block'})
                    for s in section_series:
                        if "Forthcoming series" not in str(s):
                            series_url = "https://www.espncricinfo.com" +s.get('data-summary-url')
                            # series_id = series_url.split('=')[-1]
                            series = s.find('div',{'class':'series-info'}).find('a').text
                            yield scrapy.Request(url=series_url,callback=self.parse_series,meta={'season':response.meta['season'],'series_id':s.get('data-series-id'),'series':series,'in':'1','format':frmt})



    def parse_series(self,response):
        old_matches = self.old_matches
        format = response.meta['format']

        series_id = response.meta['series_id']
        series = response.meta['series']
        resp = str(response.body)
        resp = resp.replace('\\n', '').replace('\\r', '')[2:-1]
        soup = BeautifulSoup(resp,'lxml')
        matches = soup.find_all('section', {'class': 'default-match-block '})
        if not matches:
            matches = soup.find_all('section', {'class': 'default-match-block'})

        match_ids = []
        new_matches = []

        for m in matches:
            m_id = m.find('a').get('href')
            m_id = m_id.split('scorecard/')[-1].split('/')[0]
            if "http" in str(m_id):
                m_id = m.find('a').get('href').split('game/')[-1].split('/')[0]
            if (str(m_id) not in old_matches) and ("https" not in str(m_id)):
                new_matches.append(m_id)

        if len(new_matches)>0:
            print("-------------------------------------------------------------------------------------------")
            print('Found New Matches: ',len(new_matches))
            print("-------------------------------------------------------------------------------------------")
        
        for m in matches:
            for inn in [1,2]:
                m_id = m.find('a').get('href')
                m_id = m_id.split('scorecard/')[-1].split('/')[0]
                if "http" in m_id:
                    m_id = m.find('a').get('href').split('game/')[-1].split('/')[0]
                match_ids.append(m_id)

                if (str(m_id) not in old_matches) and ("without a ball bowled" not in str(m.text.strip())):

                    ground = m.find('a').text.split(' at ')[-1].replace('(night)', '').strip()
                    m_date = m.find('div',{'class':'match-info'}).find('span',{'class':'bold'}).text
                    year = m.find('div',{'class':'match-info'})
                    if year:
                        year=year.find('span').text.split(',')[-1].strip()
                    else:
                        year = response.meta['season']
                    data = {}
                    data['mid'] = m_id
                    data['date'] = m_date
                    data['venue'] = ground
                    data['league'] = series
                    data['sid'] = series_id
                    data['year'] = year
                    data['format'] = format
                    # yield data
                    data['innings'] = inn
                    comm_url = 'https://hsapi.espncricinfo.com/v1/pages/match/comments?lang=en&leagueId={}&eventId={}&period={}&page=1&filter=full&liveTest=false'.format(series_id,m_id,inn)
                    yield scrapy.Request(url=comm_url,callback=self.comm_parse,meta={'data':data,'p_count':1},dont_filter=True)
                else:
                    # print(m_id)
                    pass


    def comm_parse(self,response):
        soup = BeautifulSoup(response.body,'lxml')
        p_count = response.meta['p_count']
        data = response.meta['data']
        json_obj = json.loads(soup.text)
        pages = json_obj['pagination']['pageCount']

        for c in json_obj['comments']:
            data['isWide'] = c['isWide']
            data['isNoball'] = c['isNoball']

            if c['isWide'] or c['isNoball']:
                data['valid'] = 0
            else:
                data['valid'] = 1

            try:
                data['bat_pid'] = c['currentBatsmen'][0]['id']
                try:
                    if data['bat_pid']:
                        data['bat_pid'] = int(data['bat_pid'])
                except:
                    pass
                if data['bat_pid']=="undefined":
                    data['bat_pid'] = None
            except:
                data['bat_pid'] = None

            try:
                data['bowl_pid'] = c['currentBowlers'][0]['id']
                try:
                    if data['bat_pid']:
                        data['bat_pid'] = int(data['bat_pid'])
                except:
                    pass
                if data['bowl_pid']=="undefined":
                    data['bowl_pid'] = None
            except:
                data['bowl_pid'] = None

            data['bp'] = int(c['over']) + 1
            if c['over']<=6:
                data['game_period'] = 1
            elif c['over']>6 and c['over']<=10:
                data['game_period'] = 2
            elif c['over']>10 and c['over']<16:
                data['game_period'] = 3
            else:
                data['game_period'] = 4

            data['ball_faced'] = c['currentBatsmen'][0]['balls']
            data['balls'] = c['ball']
            data['runs'] = c['runs']
            data['bat_runs'] = c['runs']
            if c['isWide']:
                data['bat_runs'] = 0
            elif c['isNoball']:
                data['bat_runs'] = int(c['runs']) - 1
            try:
                if ("leg bye" in c['shortText'].lower()) or ("bye" in c['shortText'].lower()):
                    data['byes'] = 1
                else:
                    data['byes'] = 0
            except:
                try:
                    if ("leg bye" in c['Text'].lower()) or ("bye" in c['Text'].lower()):
                        data['byes'] = 1
                    else:
                        data['byes'] = 0
                except:
                    data['byes'] = 0

            if data['byes']:
                c['bat_runs'] = 0
            # try:
            if c.get('matchWicket'):
                data['wickets'] = 1
                # data['out_btid'] = c['matchWicket']['id']
                if 'run out' in c['matchWicket']['text'].lower():
                    data['run_out'] = 1
                    data['out_btid'] = ""
                    for plr in c['currentBatsmen']:
                        if plr['id']!='undefined':
                            if plr['name'].split()[-1].lower() in c['matchWicket']['text'].lower():
                                data['out_btid'] = plr.get('id')
                                break
                            else:
                                data['out_btid'] = ""
                        else:
                            continue
                    # if data['out_btid'] == "":
                    #     print('WICKET: ',c['matchWicket'])
                    #     print('Players: ',c['currentBatsmen'])
                else:
                    data['out_btid'] = c['matchWicket'].get('id')
                    data['run_out'] = 0
            else:
                data['wickets'] = 0
                data['run_out'] = 0
                data['out_btid'] = ""
            # except Exception as e:
            #     print(e)
            #     data['wickets'] = 0
            #     data['out_btid'] = ""
            #     data['run_out'] = 0
            data['ra'] = 0
            if c['isBoundary'] and c['runs']>=6:
                data['sixes']=1
            else:
                data['sixes'] = 0
            data['boundry'] = c['isBoundary']
            if data['boundry']:
                data['boundry'] = 1
            else:
                data['boundry'] = 0
            for d in data.keys():
                if data[d]:
                    data[d] = self.clean_txt(str(data[d]))

            # if data['run_out']=='1' and str(data['out_btid']) != str(data['bat_pid']):
            #     spare_row = data
            #     spare_row['bat_pid'] = data['out_btid']
            #     null_keys = 'innings valid bowl_pid bp game_period ball_faced balls runs byes ra sixes boundary'.split()
            #     for ky in null_keys:
            #         spare_row[ky] = ""
            #     yield spare_row
            yield data

        if int(str(p_count))==1:
            for p in range(1,pages):
                url = response.request.url.split('&page=')[0]
                url = url + "&page={}&filter=full&liveTest=false".format(p+1)
                yield scrapy.Request(url=url,callback=self.comm_parse,meta={'data':data,'p_count':p+1})


    def clean_txt(self,txt):
        return txt.encode('ascii', 'ignore') \
            .decode('utf-8') \
            .replace('\n', '') \
            .replace('\t', '') \
            .replace('\r', '') \
            .strip()

    # Not being used
    def parse_player(self, response):
        soup = BeautifulSoup(response.body,'lxml')
        title = soup.find('div', {'class': 'player-card__details'})
        if title:
            title = title.find('h2').text.strip()
        else:
            title = ""
        country = soup.find('span', {'class': 'player-card__country-name'})
        if country:
            country = country.text.strip()
        else:
            country = ""
        divs = soup.find('div',{'class':'player_overview-grid'})
        datas = soup.find_all('div')
        bat_style = ""
        bowl_style = ""
        for d in datas:
            if "batting style" in str(d).lower():
                bat_style = d.find('h5').text
                if bat_style == "Right hand bat":
                    bat_style = 'rhb'
                elif bat_style == "Left hand bat":
                    bat_style = 'lhb'
                break
            else:
                bat_style = ""
        for d in datas:
            if "bowling style" in str(d).lower():
                bowl_style = d.find('span').text
                if bowl_style == "Right-hand bat":
                    bowl_style = 'rhb'
                elif bowl_style == "Left-hand bat":
                    bowl_style = 'lhb'
                break
            else:
                bowl_style = ""
        data = {}
        data['player'] = title
        data['country'] = country
        data['team'] = country
        data['bat_type'] = bat_style
        data['bowl_type'] = self.bowl_style(bowl_style)
        d2 = response.meta['data']
        type = response.meta['type']
        if type=='bat':
            self.unique_players[d2['bat_pid']] = data
            for k in data:
                d2["bat_" + k] = data[k]
            data=d2
            if data["bowl_pid"]:
                if str(data["bowl_pid"]) not in self.unique_players.keys():
                    url = "https://www.espncricinfo.com/pakistan/content/player/{}.html".format(data["bowl_pid"])
                    yield scrapy.Request(url=url, callback=self.parse_player,
                                         meta={'type': "bowl", 'data': data})
                else:
                    for k in self.unique_players[data["bowl_pid"]].keys():
                        data["bowl_" + k] = self.unique_players[data["bowl_pid"]][k]
            else:
                p_keys = ['player', 'country', 'team', 'bat_type', 'bowl_type']
                for k in p_keys:
                    data["bowl_" + k] = ""

            for d in data.keys():
                if data[d]:
                    data[d] = self.clean_txt(str(data[d]))
            if "bat_player" in data and "bowl_player" in data:
                yield data
            else:
                pass

        else:
            self.unique_players[d2['bowl_pid']] = data
            for k in data:
                d2["bowl_" + k] = data[k]
        if "bat_player" in d2 and "bowl_player" in d2:
            yield d2
        else:
            pass

    def bowl_style(self, bt):
        _bowl_type = bt.lower()
        _res = '-'
        if 'offbreak' in _bowl_type:
            _res = 'OB'
        elif 'slow' in _bowl_type.lower() and "left-arm" in _bowl_type.lower():
            _res = 'SLA'
        elif 'right' in _bowl_type.lower() and 'medium' in _bowl_type.lower():
            _res = 'RM'
        elif 'right' in _bowl_type.lower() and 'fast' in _bowl_type.lower():
            _res = 'RF'
        elif 'left' in _bowl_type.lower() and 'medium' in _bowl_type.lower():
            _res = 'LM'
        elif 'left' in _bowl_type.lower() and 'fast' in _bowl_type.lower():
            _res = 'LF'
        elif 'legbreak' in _bowl_type.lower():
            _res = 'LB'
        return _res



class PlayersSpider(scrapy.Spider):
    name = 'players'
    file_name = 'Players.csv'
    file_name = file_name
    allowed_domains = ['www.espncricinfo.com']
    custom_settings = {'LOG_LEVEL': 'INFO',
                       'RETRY_ENABLED': True,
                       'FEED_FORMAT': 'csv',
                       'FEED_URI': file_name,
                       'LOG_FILE': file_name.replace('csv', 'log'),
                        'DOWNLOAD_DELAY': 1,
                       'CONCURRENT_REQUESTS':30
                       }

    def start_requests(self):

        pids = []
        try:
            df = pandas.read_csv('MBData.csv')
            bids = list(df['bat_pid'].unique().tolist())
            bids2 = list(df['bowl_pid'].unique().tolist())
        except:
            bids = []
            bids2=[]

        for i in bids:
            try:
                i = str(int(i))
            except:
                continue
                i = str(i)
            if i not in pids:
                pids.append(i)

        for i in bids2:
            try:
                i = str(int(i))
            except:
                continue
                i = str(i)
            if i not in pids:
                pids.append(i)

        new_pids = []
        try:
            df = pandas.read_csv('Players.csv')
            bids = list(df['pid'].unique().tolist())
            bids = [str(x) for x in bids]
            for p in pids:
                if str(p) not in bids:
                    new_pids.append(str(p))
        except:
            new_pids = pids
        if len(new_pids)==1:
            new_pids = []
        # print('----------------------------------------------------------------------------------')
        # print('Total {} New PIDs!'.format(len(new_pids)))
        # print('----------------------------------------------------------------------------------')
        for pid in new_pids:
            if pid:
                try:
                    url = "https://www.espncricinfo.com/pakistan/content/player/{}.html".format(str(pid))
                    yield scrapy.Request(url=url,callback=self.parse,meta={'id':pid})

                except Exception as e:
                    print(e)


    def parse(self, response):
        soup = BeautifulSoup(response.body,'lxml')
        title = soup.find('div', {'class': 'player-card__details'})
        if title:
            title = title.find('h2').text.strip()
        else:
            title = ""
        country = soup.find('span', {'class': 'player-card__country-name'})
        if country:
            country = country.text.strip()
        else:
            country = ""
        divs = soup.find('div', {'class': 'player_overview-grid'})
        datas = divs.find_all('div')
        bat_style = ""
        bowl_style = ""
        for d in datas:
            if "batting style" in str(d).lower():
                bat_style = d.find('h5').text
                if bat_style == "Right hand bat":
                    bat_style = 'rhb'
                elif bat_style == "Left hand bat":
                    bat_style = 'lhb'
                break
            else:
                bat_style = ""
        for d in datas:
            if "bowling style" in str(d).lower():
                bowl_style = d.find('h5').text
                break
            else:
                bowl_style = ""
        data = {}
        data['pid'] = response.meta['id']
        data['player'] = title
        data['country'] = country
        data['team'] = country
        data['bat_type'] = bat_style
        data['bowl_type'] = self.bowl_style(bowl_style)
        for d in data.keys():
            data[d] = self.clean_txt(data[d])
        yield data

    def clean_txt(self,txt):
        return txt.encode('ascii', 'ignore') \
            .decode('utf-8') \
            .replace('\n', '') \
            .replace('\t', '') \
            .replace('\r', '') \
            .strip()


    def bowl_style(self, bt):
        _bowl_type = bt.lower()
        _res = '-'
        if 'offbreak' in _bowl_type:
            _res = 'OB'
        elif 'slow' in _bowl_type.lower() and "left arm" in _bowl_type.lower():
            _res = 'SLA'
        elif 'left' in _bowl_type.lower() and "wrist" in _bowl_type.lower():
            _res = "LWS"
        elif 'right' in _bowl_type.lower() and 'medium' in _bowl_type.lower():
            _res = 'RM'
        elif 'right' in _bowl_type.lower() and 'fast' in _bowl_type.lower():
            _res = 'RF'
        elif 'left' in _bowl_type.lower() and 'medium' in _bowl_type.lower():
            _res = 'LM'
        elif 'left' in _bowl_type.lower() and 'fast' in _bowl_type.lower():
            _res = 'LF'
        elif 'legbreak' in _bowl_type.lower():
            _res = 'LB'
        return _res



class MatchData(scrapy.Spider):
    name = 'matches'
    file_name = 'Matches_meta.csv'
    file_name = file_name
    allowed_domains = ['www.espncricinfo.com']
    custom_settings = {'LOG_LEVEL': 'INFO',
                       'RETRY_ENABLED': True,
                       'FEED_FORMAT': 'csv',
                       'FEED_URI': file_name,
                       'LOG_FILE': file_name.replace('csv', 'log'),
                       'DOWNLOAD_DELAY': 1,
                       'CONCURRENT_REQUESTS': 30
                       }

    def start_requests(self):
        try:
            df = pandas.read_csv('MBData.csv', low_memory=False)
            m_ids = list(set(df['mid'].dropna().tolist()))
        except:
            m_ids = []
        # print('----------------------------------------------------------------------------------')
        # print(f"Total match data retrieved from MBData : {len(m_ids)}")
        # print('----------------------------------------------------------------------------------')
        to_do_mids = []
        try:
            df_meta = pandas.read_csv('Matches_meta.csv')
            done_mids = list(set(df_meta['mid'].dropna().tolist()))
        except:
            done_mids = []

        for m in m_ids:
            if str(m) not in done_mids:
                to_do_mids.append(m)

        print('----------------------------------------------------------------------------------')
        print(f"Total match data to retrieve : {len(to_do_mids)}")
        print('----------------------------------------------------------------------------------')
        
        for m in to_do_mids:
            try:
                url = 'http://www.espncricinfo.com/ci/content/match/{}.html'.format(str(int(m)))
                yield scrapy.Request(url=url,callback=self.match_parse,meta={'m':m})
            except:
                continue


    def match_parse(self,response):
        url = str(response.request.url).replace('/live-cricket-score','/full-scorecard')
        yield scrapy.Request(url=url,callback=self.match_parse_2,meta={'m':response.meta['m']})


    def match_parse_2(self,response):
        soup = BeautifulSoup(response.body, 'lxml')

        meta_data = json.loads(soup.find(id='__NEXT_DATA__').text)
        team_dict = {}
        try:
            teams = meta_data['props']['pageProps']['data']['pageData']['match']['teams']
            for team in teams:
                team_dict[team["team"]["id"]] = team["team"]["longName"]
        except:
            team_dict = {}

        ddds = {}
        ddds['mid'] = response.meta['m']
        
        try:
            date = meta_data['props']['pageProps']['data']['pageData']['match']['startDate']
            ddds['Date'] = pandas.to_datetime(date).date()
        except:
            dds['Date'] = None

        try:
            toss_winner = meta_data['props']['pageProps']['data']['pageData']['match']['tossWinnerTeamId']
            ddds['Toss Winner'] = team_dict[toss_winner]
        except:
            ddds['Toss Winner'] = None

        try:
            winner_id = meta_data['props']['pageProps']['data']['pageData']['match']['winnerTeamId']
            ddds['Match WInner'] = team_dict[winner_id]
        except:
            ddds['Match WInner'] = None

        try:
            tossChoice = meta_data['props']['pageProps']['data']['pageData']['match']['tossWinnerChoice']
            ddds['Toss Decision'] = "Bat" if tossChoice==1 else "Field"
        except:
            ddds['Toss Decision'] = None

        try:
            ddds['Venue'] = meta_data['props']['pageProps']['data']['pageData']['match']['ground']['name']
        except:
            ddds['Venue'] = None
        
        try:
            ddds['Team 1'] = list(team_dict.values())[0]
        except:
            ddds['Team 1'] = None
        
        try:
            ddds['Team 2'] = list(team_dict.values())[1]
        except:
            ddds['Team 2'] = None

        yield ddds



if __name__=="__main__":
    # # try:
    configure_logging()
    runner = CrawlerRunner()
    path = pathlib.Path(os.getcwd())
    @defer.inlineCallbacks
    def crawl():
        print('+++++++++++++++++++++++//+Starting to Pull New Match Data+//+++++++++++++++++++++++')
        try:
            yield runner.crawl(CricinfoSpider)
        except:
            pass

        try:
            df_b = pandas.read_csv('MBData.csv', index_col=None, low_memory=False)
            try:
                i = df_b[(df_b.bp == 'bp')].index
                df_b = df_b.drop(i)
            except:
                pass
            df_b.drop_duplicates(inplace=True)
            df_b.to_csv('MBData.csv',index=None)
        except Exception as e:
            print('Exception here: {}'.format(e))
            pass

        print('Done fetching the balls data!')

        print('----------------------------------------------------------------------------------')
        print('+++++++++++++++++++++++//+Starting to Pull New Players Data+//+++++++++++++++++++++++')
        try:
            yield runner.crawl(PlayersSpider)
        except:
            pass

        print('Running scraper for Grounds Data')
        print('----------------------------------------------------------------------------------')
        print('+++++++++++++++++++++++//+Starting to Pull Match Pages+//+++++++++++++++++++++++')
        try:
            yield runner.crawl(MatchData)
        except:
            pass
        print('-----------------------------------------ALL DONE!-------------------------------')

        reactor.stop()


    crawl()
    reactor.run()

    df_b = pandas.read_csv('MBData.csv', index_col=None, low_memory=False)
    try:
        i = df_b[(df_b.bp == 'bp')].index
        df_b = df_b.drop(i)
    except:
        pass
    df_b.drop_duplicates(inplace=True)
    df_b.to_csv("MBData.csv", index=False, encoding='utf-8')

    df_p = pandas.read_csv('Players.csv', index_col=None)
    try:
        i = df_p[(df_p.pid == 'pid')].index
        df_p = df_p.drop(i)
    except:
        pass
    df_p.sort_values("pid", inplace=True)
    df_p.drop_duplicates(inplace=True)

    df_p.to_csv("Players.csv", index=False, encoding='utf-8')

    print('+++++++++++++++++++++++//+Creating CSV Extracts for Tableau+//+++++++++++++++++++++++')

    df_md = df_b[['mid','league','venue']].drop_duplicates(keep='first')
    df_md['venue'] = [str(x).replace(' (day/night)','').strip() for x in df_md['venue'].tolist()]
    df_md.to_csv((path.joinpath('matchData.csv') ),index=False)

    df_bt = df_p[['pid','player','team','country','bat_type']].drop_duplicates(keep='first')
    df_bt.columns = ['bat_pid','player','team','country','bat_type']
    df_bt.to_csv((path.joinpath('playerBatData.csv')),index=False)
    df_bl = df_p[['pid', 'player', 'team', 'country', 'bowl_type']].drop_duplicates(keep='first')
    df_bl.columns = ['bowl_pid', 'player', 'team', 'country', 'bowl_type']
    df_bl.to_csv((path.joinpath('playerBallData.csv')),index=False)

    df_bbd = df_b[['mid','date','valid','innings','bat_pid','bowl_pid','bp','game_period','ball_faced','balls','runs','wickets','ra',
                   'sixes','year','byes','out_btid','run_out','boundry','format','isNoball','isWide','bat_runs']].drop_duplicates(keep='first')

    df_bbd[['innings','bat_pid','bp','ball_faced','balls']] = df_bbd[['innings','bat_pid','bp','ball_faced','balls']].astype(float)
    df_bbd = df_bbd.astype(str)
    matches = df_bbd['mid'].unique().tolist()
    df_out = []
    for match in tqdm(matches):
        one_match = df_bbd[df_bbd['mid'] == str(match)]
        # Before
        # one_match = one_match.fillna('')
        one_match = one_match.replace('nan','')
        one_match['bp'] = one_match['bp'].replace('','0')
        one_match['bp'] = one_match['bp'].astype(float)
        # before
        # one_match = one_match.fillna('')
        one_match['bp'] = one_match['bp'].fillna(0)
        try:
            one_match['bp'] = one_match['bp'].astype(int,errors='ignore')
        except Exception as e:
            print(e)

        one_match = one_match.sort_values(['innings', 'bp', 'balls'], ascending=[True, True, True])
        bat_order_1 = []
        bat_order_2 = []
        dicts = list(one_match.T.to_dict().values())
        for k,row in enumerate(list(one_match.values)):
            row = [str(x) for x in list(row)]
            if dicts[k]['bat_pid'] != "":
                if len(dicts[k]['bat_pid']) > 1:
                    if dicts[k]['innings'] == str(1.0):
                        if bat_order_1 == [] and dicts[k]['bp'] == 0 and dicts[k]['balls'] == '1.0':
                            bat_order_1.append(dicts[k]['bat_pid'])
                        elif dicts[k]['bat_pid'] not in bat_order_1 and (
                                                dicts[k]['ball_faced'] == '0.0' or dicts[k]['ball_faced'] == '1.0' or dicts[k]['ball_faced'] == '2.0' or dicts[k]['ball_faced'] == '3.0'):
                            bat_order_1.append(dicts[k]['bat_pid'])
                    elif dicts[k]['innings'] == str(2.0):
                        if bat_order_2 == [] and dicts[k]['bp'] == 0 and dicts[k]['balls'] == '1.0':
                            bat_order_2.append(dicts[k]['bat_pid'])
                        elif dicts[k]['bat_pid'] not in bat_order_2 and (
                                                dicts[k]['ball_faced'] == '0.0' or dicts[k]['ball_faced'] == '1.0' or dicts[k]['ball_faced'] == '2.0' or dicts[k]['ball_faced'] == '3.0'):
                            bat_order_2.append(dicts[k]['bat_pid'])
                    else:
                        pass
            else:
                continue

        spare = []
        for k,row in enumerate(list(one_match.values)):
            row = [str(x) for x in list(row)]
            try:
                if dicts[k]['innings'] == str(1.0):
                    if dicts[k]['bat_pid'] != "":
                        spare.append(bat_order_1.index(dicts[k]['bat_pid']) + 1)
                    else:
                        spare.append(0)
                elif dicts[k]['innings'] == str(2.0):
                    if dicts[k]['bat_pid'] != "":
                        spare.append(bat_order_2.index(dicts[k]['bat_pid']) + 1)
                    else:
                        spare.append(0)
            except Exception as e:
                # print(e)
                spare.append(0)
        try:
            one_match['bat_ord'] = spare
        except:
            one_match['bat_ord'] = ["" for _ in range(len(one_match))]
        df_out.append(one_match)
    df_bbd = pandas.concat(df_out, ignore_index=True)
    # try:
    #     df_b.sort_values("mid",inplace=True)
    # except:
    #     print('Sorting df_b issue!')
    df_bbd.drop_duplicates(inplace=True)
    # before
    # df_bbd['bat_pid'] = [int(float(x)) if (str(x)!="nan" or str(x)!="NaN" or (not pd.isna(x))) else "" for x in df_bbd['bat_pid'].to_list()]
    # df_bbd['bowl_pid'] = [int(float(x)) if (str(x)!="nan" or str(x)!="NaN" or (not pd.isna(x))) else "" for x in df_bbd['bowl_pid'].to_list()]
    # df_bbd['out_btid'] = [int(float(x)) if (str(x)!="nan" or str(x)!="NaN" or (not pd.isna(x))) else "" for x in df_bbd['out_btid'].to_list()]
    
    s = (df_bbd.value_counts(subset='out_btid'))
    x = df_bbd.merge(s.rename('dismissals'), left_index=True, right_index=True)

    df_bbd = df_bbd.replace('nan','')
    # df_bbd = df_bbd.fillna('')
    print('Creating Blank Run out rows')
    df_bbd['run_out'] = df_bbd['run_out'].astype(float)
    df_bbd['run_out'] = df_bbd['run_out'].astype(int)
    df_sub = df_bbd[df_bbd['run_out'] == 1]
    df_sub = df_bbd[df_bbd['bat_pid']!=df_bbd['out_btid']]
    data_dicts = df_sub.to_dict('records')
    new_dicts = []
    for dd in data_dicts:
        spare_row = dd
        spare_row['bat_pid'] = dd['out_btid']
        null_keys = 'bowl_pid ball_faced balls runs byes ra sixes boundary'.split()
        for ky in null_keys:
            spare_row[ky] = ""
        new_dicts.append(spare_row)
    df_new = pandas.DataFrame(new_dicts)
    df_bbd = pandas.concat([df_bbd,df_new],ignore_index=True)
    df_bbd.rename(columns = {'date':'date1'}, inplace = True)
    df_bbd.to_csv((path.joinpath('mainBallData.csv')),index=False)

    df = pandas.read_csv('MBData.csv', low_memory=False)
    df_meta = pandas.read_csv('Matches_meta.csv', low_memory=False)
    mids = df['mid'].dropna().unique().astype(float).astype(int).tolist()

    done_dfs = []

    for m in mids:
        mid_df = df[df['mid'] == m]
        mid_dict = mid_df.to_dict('records')[0]
        date = mid_dict["date"]
        venue = mid_dict['venue']
        game = mid_dict['format']
        for inn in mid_df['innings'].unique().tolist():
            summed = mid_df[mid_df['innings'] == inn].groupby(['bp'])[['runs', 'wickets', 'sixes']].apply(sum).reset_index()
            summed['mid'] = [m for _ in range(len(summed))]
            summed['innings'] = [inn for _ in range(len(summed))]
            # summed['date'] = [date for _ in range(len(summed))]
            summed['format'] = [game for _ in range(len(summed))]
            done_dfs.append(summed)
    dfs = pandas.concat(done_dfs)
    dfs = dfs.astype(str)
    df_meta = df_meta.astype(str)
    final_df = dfs.merge(df_meta, on='mid')


    def f(row):
        if row['innings'] == 1:
            if row['Toss Decision'] == 'bat':
                val = row['Team 1']
            else:
                val = row['Team 2']
            return val
        else:
            if row['Toss Decision'] == 'bat':
                val = row['Team 2']
            else:
                val = row['Team 1']
            return val


    final_df['Batting Team'] = final_df.apply(f, axis=1)

    final_df.columns = ['over', 'runs_scored', 'wickets_lost', 'sixes', 'mid', 'innings',
                        'competition', 'date', 'toss_winner', 'match_winner', 'toss_decision',
                        'Venue', 'team_1', 'team_2', 'batting_team']


    di = {'Twenty20': 'T20', 'Twenty20 Internationals': 'T20I'}
    final_df = final_df.replace({'competition': di})
    final_df.to_csv('grounds_data.csv', index=False)


    print('All Done!')
    input('Press any key to close the window!')